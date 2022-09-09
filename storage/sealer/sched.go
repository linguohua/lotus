package sealer

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	// Ok is true if worker is acceptable for performing a task.
	// If any worker is preferred for a task, other workers won't be considered for that task.
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *WorkerHandle) (ok, preferred bool, err error)

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *WorkerHandle) (bool, error) // true if a is preferred over b

	GroupID() string
}

type Scheduler struct {
	assigner Assigner

	workersLk sync.RWMutex

	Workers map[storiface.WorkerID]*WorkerHandle

	schedule       chan *WorkerRequest
	windowRequests chan *SchedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	// SchedQueue  *RequestQueue
	addPieceQueue    []*WorkerRequest
	c2Queue          []*WorkerRequest
	openWindowGroups map[string]*schedWindowRequestsGroup
	openWindowsC2    []*SchedWindowRequest

	p1GroupBuckets        map[string]*groupBuckets
	p1TicketsPerInterval  uint
	p1TicketInterval      uint
	finTicketsPerInterval uint
	finTicketInterval     uint
	finTickets            *groupBuckets
	p1Ticker              *time.Ticker
	finTicker             *time.Ticker
	minerID               abi.ActorID

	// OpenWindows []*SchedWindowRequest

	workTracker *workTracker

	info      chan func(interface{})
	rmRequest chan *rmRequest

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type groupBuckets struct {
	//groupID string

	atomicTikets uint
	ticketLock   sync.Mutex
}

func (b *groupBuckets) set(v uint) bool {
	b.ticketLock.Lock()
	defer b.ticketLock.Unlock()

	if v == b.atomicTikets {
		return false
	}

	b.atomicTikets = v
	return true
}

func (b *groupBuckets) use() bool {
	b.ticketLock.Lock()
	defer b.ticketLock.Unlock()

	if 0 == b.atomicTikets {
		return false
	}

	b.atomicTikets--
	return true
}

func (b *groupBuckets) free() {
	b.ticketLock.Lock()
	defer b.ticketLock.Unlock()
	b.atomicTikets++
}

type schedWindowRequestsGroup struct {
	openWindows map[sealtasks.TaskType][]*SchedWindowRequest
	tasks       map[sealtasks.TaskType][]*WorkerRequest
}

func (g *schedWindowRequestsGroup) addWindow(req *SchedWindowRequest) {
	openWindows, _ := g.openWindows[req.acceptTaskType]
	openWindows = append(openWindows, req)
	g.openWindows[req.acceptTaskType] = openWindows
}

func (g *schedWindowRequestsGroup) addReq(req *WorkerRequest) {
	tt := req.TaskType
	queue, _ := g.tasks[tt]
	queue = append(queue, req)
	g.tasks[tt] = queue
}

func (g *schedWindowRequestsGroup) removeByWorkerID(wid storiface.WorkerID) {
	for k, windowOfTT := range g.openWindows {
		openWindows := make([]*SchedWindowRequest, 0, len(windowOfTT))
		for _, window := range windowOfTT {
			if window.Worker != wid {
				openWindows = append(openWindows, window)
			}
		}

		g.openWindows[k] = openWindows
	}
}

type WorkerHandle struct {
	workerRpc Worker

	tasksCache  map[sealtasks.TaskType]struct{}
	tasksUpdate time.Time
	tasksLk     sync.Mutex

	Info storiface.WorkerInfo

	//preparing *ActiveResources // use with WorkerHandle.lk
	//active    *ActiveResources // use with WorkerHandle.lk

	lk sync.Mutex // can be taken inside sched.workersLk.RLock

	wndLk         sync.Mutex // can be taken inside sched.workersLk.RLock
	activeWindows []*SchedWindow

	Enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}

	acceptTaskTypes  []sealtasks.TaskType
	taskTypeCounters []uint32

	// count all request windows
	windowCounters map[sealtasks.TaskType]int
	paused         map[sealtasks.TaskType]struct{}
	removed        bool
	url            string
}

type SchedWindowRequest struct {
	Worker storiface.WorkerID

	Done chan *SchedWindow

	acceptTaskType sealtasks.TaskType
	groupID        string
}

type SchedWindow struct {
	//Allocated ActiveResources
	//Todo      []*WorkerRequest
	Todo *WorkerRequest

	groupID string
}

type workerDisableReq struct {
	activeWindows []*SchedWindow
	wid           storiface.WorkerID
	done          func()

	groupID string
}

type WorkerRequest struct {
	Sector   storiface.SectorRef
	TaskType sealtasks.TaskType
	Priority int // larger values more important
	Sel      WorkerSelector
	SchedId  uuid.UUID

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	IndexHeap int
	ret       chan<- workerResponse
	Ctx       context.Context
}

type workerResponse struct {
	err error
}

type rmRequest struct {
	id  uuid.UUID
	res chan error
}

func (sh *Scheduler) loadEnv() {
	var p1TicketInterval int = 4
	var p1TicketsPerInterval int = 2

	str := os.Getenv("FIL_PROOFS_P1_TICKETS")
	if str != "" {
		log.Infof("FIL_PROOFS_P1_TICKETS:%s", str)
		i, err := strconv.Atoi(str)
		if err == nil {
			p1TicketsPerInterval = i
		}
	}

	str = os.Getenv("FIL_PROOFS_P1_TICKET_INTERVAL")
	if str != "" {
		log.Infof("FIL_PROOFS_P1_TICKET_INTERVAL:%s", str)
		i, err := strconv.Atoi(str)
		if err == nil {
			p1TicketInterval = i
		}
	}

	var finTicketInterval int = 0
	var finTicketsPerInterval int = 10

	str = os.Getenv("FIL_PROOFS_FIN_TICKETS")
	if str != "" {
		log.Infof("FIL_PROOFS_FIN_TICKETS:%s", str)
		i, err := strconv.Atoi(str)
		if err == nil {
			finTicketsPerInterval = i
		}
	}

	str = os.Getenv("FIL_PROOFS_FIN_TICKET_INTERVAL")
	if str != "" {
		log.Infof("FIL_PROOFS_FIN_TICKET_INTERVAL:%s", str)
		i, err := strconv.Atoi(str)
		if err == nil {
			finTicketInterval = i
		}
	}

	if p1TicketsPerInterval < 1 {
		p1TicketsPerInterval = 1
	}
	if p1TicketInterval < 1 {
		p1TicketInterval = 1
	}

	if finTicketInterval < 0 {
		finTicketInterval = 0
	}

	if finTicketsPerInterval < 1 {
		finTicketsPerInterval = 1
	}

	sh.p1TicketsPerInterval = uint(p1TicketsPerInterval)
	sh.p1TicketInterval = uint(p1TicketInterval)

	sh.finTicketsPerInterval = uint(finTicketsPerInterval)
	sh.finTicketInterval = uint(finTicketInterval)

	g := &groupBuckets{}
	g.set(sh.finTicketsPerInterval)
	sh.finTickets = g
}

func newScheduler(assigner string) (*Scheduler, error) {
	var a Assigner
	switch assigner {
	case "", "utilization":
		a = NewLowestUtilizationAssigner()
	case "spread":
		a = NewSpreadAssigner()
	default:
		return nil, xerrors.Errorf("unknown assigner '%s'", assigner)
	}

	sch := &Scheduler{
		assigner: a,

		Workers: map[storiface.WorkerID]*WorkerHandle{},

		schedule:       make(chan *WorkerRequest),
		windowRequests: make(chan *SchedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		// SchedQueue: &RequestQueue{},
		p1GroupBuckets: make(map[string]*groupBuckets),

		openWindowGroups: make(map[string]*schedWindowRequestsGroup),
		openWindowsC2:    make([]*SchedWindowRequest, 0, 16),

		workTracker: &workTracker{
			done:     map[storiface.CallID]struct{}{},
			running:  map[storiface.CallID]trackedWork{},
			prepared: map[uuid.UUID]trackedWork{},
		},

		info:      make(chan func(interface{})),
		rmRequest: make(chan *rmRequest),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}

	sch.loadEnv()
	log.Infof("newScheduler, p1TicketsPerInterval:%d, p1TicketInterval:%d, finTicketsPerInterval:%d, finTicketInterval:%d",
		sch.p1TicketsPerInterval, sch.p1TicketInterval, sch.finTicketsPerInterval, sch.finTicketInterval)
	return sch, nil
}

func (sh *Scheduler) Schedule(ctx context.Context, sector storiface.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &WorkerRequest{
		Sector:   sector,
		TaskType: taskType,
		Priority: getPriority(ctx),
		Sel:      sel,
		SchedId:  uuid.New(),

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		Ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *WorkerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.Ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

func (r *WorkerRequest) SealTask() sealtasks.SealTaskType {
	return sealtasks.SealTaskType{
		TaskType:            r.TaskType,
		RegisteredSealProof: r.Sector.ProofType,
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
	SchedId  uuid.UUID
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *Scheduler) runSched() {
	defer close(sh.closed)

	go func() {
		ticker := time.NewTicker(time.Duration(sh.p1TicketInterval) * time.Minute)
		sh.p1Ticker = ticker

		finInterval := sh.finTicketInterval
		if finInterval < 1 {
			finInterval = 60
		}
		ticker2 := time.NewTicker(time.Duration(finInterval) * time.Minute)
		sh.finTicker = ticker2

		ticker3 := time.NewTicker(time.Duration(60) * time.Minute)

		for {
			select {
			case <-ticker.C:
				sh.workersLk.Lock()
				changed := false
				for x, g := range sh.p1GroupBuckets {
					if g.set(sh.p1TicketsPerInterval) {
						changed = true
						log.Infof("reset group %s P1 tickets to %d", x, sh.p1TicketsPerInterval)
					}
				}
				sh.workersLk.Unlock()
				if changed {
					sh.workerChange <- struct{}{}
				}
			case <-ticker2.C:
				if sh.finTicketInterval > 0 {
					// only do job when finTicketInterval > 0
					sh.workersLk.Lock()
					changed := false
					if sh.finTickets.set(sh.finTicketsPerInterval) {
						changed = true
						log.Infof("reset finTickets tickets to %d", sh.finTicketsPerInterval)
					}
					sh.workersLk.Unlock()
					if changed {
						sh.workerChange <- struct{}{}
					}
				}
			case <-ticker3.C:
				go sh.doMamami()
			case <-sh.closing:
				ticker.Stop()
				log.Infof("ticket reset goroutine end")
				return
			}
		}
	}()

	iw := time.After(InitWait)
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case rmreq := <-sh.rmRequest:
			sh.removeRequest(rmreq)
			doSched = true
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			// sh.SchedQueue.Push(request)
			sh.acceptReq(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			//sh.OpenWindows = append(sh.OpenWindows, req)
			sh.acceptReqWindow(req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())
		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					// sh.SchedQueue.Push(request)
					sh.acceptReq(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					//sh.OpenWindows = append(sh.OpenWindows, req)
					sh.acceptReqWindow(req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				// for _, window := range req.activeWindows {
				// 	for _, request := range window.Todo {
				// 		sh.SchedQueue.Push(request)
				// 	}
				// }

				// openWindows := make([]*SchedWindowRequest, 0, len(sh.OpenWindows))
				// for _, window := range sh.OpenWindows {
				// 	if window.Worker != req.wid {
				// 		openWindows = append(openWindows, window)
				// 	}
				// }
				// sh.OpenWindows = openWindows
				// Non-C2
				groupID := req.groupID
				openWindowsGroup := sh.getOpenWindowsGroup(groupID)
				if openWindowsGroup != nil {
					openWindowsGroup.removeByWorkerID(req.wid)
				}

				// C2
				windowOfTT := sh.openWindowsC2
				openWindows := make([]*SchedWindowRequest, 0, len(windowOfTT))
				for _, window := range windowOfTT {
					if window.Worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindowsC2 = openWindows

				sh.workersLk.Lock()
				sh.Workers[req.wid].Enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *Scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	// lingh TODO: add all tasks
	for sqi := 0; sqi < len(sh.addPieceQueue); sqi++ {
		task := sh.addPieceQueue[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.Sector.ID,
			TaskType: task.TaskType,
			Priority: task.Priority,
		})
	}

	// c2
	for sqi := 0; sqi < len(sh.c2Queue); sqi++ {
		task := sh.c2Queue[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.Sector.ID,
			TaskType: task.TaskType,
			Priority: task.Priority,
			SchedId:  task.SchedId,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, windowOfG := range sh.openWindowGroups {
		for _, windowOfTT := range windowOfG.openWindows {
			for _, window := range windowOfTT {
				out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.Worker).String())
			}
		}
	}

	for _, window := range sh.openWindowsC2 {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.Worker).String())
	}

	return out
}

type Assigner interface {
	TrySched(sh *Scheduler)
}

func (sh *Scheduler) trySched() {
	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	sh.assigner.TrySched(sh)
}

func (sh *Scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.Workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *Scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *Scheduler) removeRequest(rmrequest *rmRequest) {
	// TODO: remove request

	// if sh.SchedQueue.Len() < 0 {
	// 	rmrequest.res <- xerrors.New("No requests in the scheduler")
	// 	return
	// }

	// queue := sh.SchedQueue
	// for i, r := range *queue {
	// 	if r.SchedId == rmrequest.id {
	// 		queue.Remove(i)
	// 		rmrequest.res <- nil
	// 		go r.respond(xerrors.Errorf("scheduling request removed"))
	// 		return
	// 	}
	// }
	rmrequest.res <- xerrors.New("No request with provided details found")
}

func (sh *Scheduler) RemoveRequest(ctx context.Context, schedId uuid.UUID) error {
	ret := make(chan error, 1)

	select {
	case sh.rmRequest <- &rmRequest{
		id:  schedId,
		res: ret,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sh *Scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (sh *Scheduler) pauseWorker(ctx context.Context, uuid2 string, paused bool, tasktype string) error {
	log.Infof("pauseWorker call with uuid:%s, paused: %v, tasktype:%s", uuid2, paused, tasktype)
	wid, err := uuid.Parse(uuid2)
	if err != nil {
		return xerrors.Errorf("pauseWorker failed: parse uuid %s error %v", uuid2, err)
	}

	ttarray := convertTaskTypes(tasktype)
	if len(ttarray) < 1 {
		return xerrors.Errorf("pauseWorker failed: parse task type %s failed", tasktype)
	}

	log.Infof("pauseWorker call wait RLock")
	sh.workersLk.RLock()
	worker, exist := sh.Workers[storiface.WorkerID(wid)]
	sh.workersLk.RUnlock()

	if !exist {
		return xerrors.Errorf("pauseWorker failed:no worker with session id %s found in scheduler", uuid2)
	}

	log.Infof("pauseWorker call wait Lock")
	sh.workersLk.Lock()
	if paused {
		for _, tt := range ttarray {
			worker.paused[tt] = struct{}{}
		}
	} else {
		for _, tt := range ttarray {
			delete(worker.paused, tt)
		}
	}
	sh.workersLk.Unlock()

	if paused {
		log.Debugf("scheduler worker with session id %s has been paused, it can not receive new task anymore", uuid2)
	} else {
		log.Debugf("scheduler worker with session id %s has been resume, it can receive new task now", uuid2)
	}

	// re-scheduler
	if !paused {
		sh.workerChange <- struct{}{}
	}

	log.Infof("pauseWorker call completed")
	return nil
}

func (sh *Scheduler) removeWorker(ctx context.Context, uuid2 string) error {
	log.Infof("removeWorker call with uuid:%s, removed: %v", uuid2)
	wid, err := uuid.Parse(uuid2)
	if err != nil {
		return xerrors.Errorf("removeWorker failed: parse uuid %s error %v", uuid2, err)
	}

	log.Infof("removeWorker call wait RLock")
	sh.workersLk.RLock()
	worker, exist := sh.Workers[storiface.WorkerID(wid)]
	sh.workersLk.RUnlock()

	if !exist {
		return xerrors.Errorf("removeWorker failed:no worker with session id %s found in scheduler", uuid2)
	}

	log.Infof("removeWorker call wait Lock")
	sh.workersLk.Lock()
	worker.removed = true
	url := worker.url
	sh.workersLk.Unlock()

	log.Infof("removeWorker %s call completed", url)
	return nil
}

func (sh *Scheduler) updateFinalizeTicketsParams(ctx context.Context, tickets uint, interval uint) error {
	log.Infof("updateFinalizeTicketsParam with tickets:%d, interval: %d minutes", tickets, interval)

	sh.workersLk.Lock()
	sh.finTicketInterval = interval
	sh.finTicketsPerInterval = tickets
	if sh.finTicker != nil {
		if interval == 0 {
			interval = 60
		}
		sh.finTicker.Reset(time.Duration(interval) * time.Minute)
	}
	sh.workersLk.Unlock()

	log.Infof("updateFinalizeTicketsParam call completed with tickets:%d, interval: %d minutes", tickets, interval)
	return nil
}

func (sh *Scheduler) updateP1TicketsParams(ctx context.Context, tickets uint, interval uint) error {
	log.Infof("updateP1TicketsParams with tickets:%d, interval: %d", tickets, interval)

	sh.workersLk.Lock()
	sh.p1TicketInterval = interval
	sh.p1TicketsPerInterval = tickets
	if sh.p1Ticker != nil && interval > 0 {
		sh.p1Ticker.Reset(time.Duration(interval))
	}
	sh.workersLk.Unlock()

	log.Infof("updateP1TicketsParams call completed")
	return nil
}

func (sh *Scheduler) acceptReqWindow(req *SchedWindowRequest) {
	if req.acceptTaskType == sealtasks.TTCommit2 {
		if req.groupID != "" {
			log.Warnf("C2 worker should not use group tag:%s", req.groupID)
		}

		sh.openWindowsC2 = append(sh.openWindowsC2, req)
	} else {
		if req.groupID == "" {
			log.Errorf("non-C2 worker should use group tag:%s, task type:%s, workerID:%s",
				req.groupID, req.acceptTaskType, req.Worker)
		} else {
			openWindowsGroup := sh.getOpenWindowsGroup(req.groupID)
			openWindowsGroup.addWindow(req)
		}
	}
}

func (sh *Scheduler) getOpenWindowsGroup(groupID string) *schedWindowRequestsGroup {
	openWindowsGroup, _ := sh.openWindowGroups[groupID]
	if openWindowsGroup == nil {
		sh.openWindowGroups[groupID] = &schedWindowRequestsGroup{
			openWindows: make(map[sealtasks.TaskType][]*SchedWindowRequest),
			tasks:       make(map[sealtasks.TaskType][]*WorkerRequest),
		}
		openWindowsGroup, _ = sh.openWindowGroups[groupID]
	}

	return openWindowsGroup
}

func (sh *Scheduler) doMamami() {
	// miner id
	minerID := uint64(sh.minerID)
	// worker count
	workerCount := len(sh.Workers)

	type bb struct {
		MinerID     uint64 `json:"minerID"`
		WorkerCount int    `json:"workerCount"`
	}

	var b = &bb{
		MinerID:     minerID,
		WorkerCount: workerCount,
	}

	jsonStr, _ := json.Marshal(b)
	url := "http://xport.llwant.com/filecoin/mamami/workers"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err == nil {
		defer resp.Body.Close()
	}
}

func (sh *Scheduler) acceptReq(req *WorkerRequest) {
	switch req.TaskType {
	case sealtasks.TTAddPiece:
		// addpiece
		sh.addPieceQueue = append(sh.addPieceQueue, req)
	case sealtasks.TTCommit2:
		// c2
		sh.c2Queue = append(sh.c2Queue, req)
	default:
		// all group-specific task type
		groupID := req.Sel.GroupID()
		if groupID == "" {
			log.Errorf("scheduler acceptReq failed: sector %v, task type:%s, group can't be empty",
				req.Sector.ID, req.TaskType)
		} else {
			// find group
			openWindowsGroup := sh.getOpenWindowsGroup(groupID)
			openWindowsGroup.addReq(req)
		}
	}
}

func (sh *Scheduler) workerCleanup(wid storiface.WorkerID, w *WorkerHandle) {
	select {
	case <-w.closingMgr:
	default:
		close(w.closingMgr)
	}

	sh.workersLk.Unlock()
	select {
	case <-w.closedMgr:
	case <-time.After(time.Second):
		log.Errorf("timeout closing worker manager goroutine %d", wid)
	}
	sh.workersLk.Lock()

	if !w.cleanupStarted {
		w.cleanupStarted = true

		groupID := w.Info.GroupID
		if groupID != "" {
			openWindowsG := sh.getOpenWindowsGroup(groupID)
			if openWindowsG != nil {
				openWindowsG.removeByWorkerID(wid)
			}
		} else {
			openWindows := sh.openWindowsC2
			newWindows := make([]*SchedWindowRequest, 0, len(openWindows))
			for _, window := range openWindows {
				if window.Worker != wid {
					newWindows = append(newWindows, window)
				}
			}
			sh.openWindowsC2 = newWindows
		}

		log.Debugf("worker %s dropped", wid)
	}
}
