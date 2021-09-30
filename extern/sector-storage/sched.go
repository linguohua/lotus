package sectorstorage

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
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 3 * time.Second
var InitWait = 3 * time.Second

var (
// SchedWindows = 2
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
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b

	GroupID() string
}

type groupBuckets struct {
	//groupID string

	atomicTikets uint
	ticketLock   sync.Mutex
}

type schedWindowRequestsGroup struct {
	openWindows map[sealtasks.TaskType][]*schedWindowRequest
	tasks       map[sealtasks.TaskType][]*workerRequest
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	p1GroupBuckets        map[string]*groupBuckets
	p1TicketsPerInterval  uint
	p1TicketInterval      uint
	finTicketsPerInterval uint
	finTicketInterval     uint
	finTickets            *groupBuckets
	p1Ticker              *time.Ticker
	finTicker             *time.Ticker

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	// schedQueue *requestQueue
	addPieceQueue []*workerRequest
	c2Queue       []*workerRequest

	openWindowGroups map[string]*schedWindowRequestsGroup
	openWindowsC2    []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing

	finalizeTicks int

	minerID abi.ActorID
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	acceptTaskTypes     []sealtasks.TaskType
	taskTypeValidcounts []uint32

	//preparing *activeResources
	//active *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow
	// count all request windows
	requestedWindowsCounter map[sealtasks.TaskType]int

	paused map[sealtasks.TaskType]struct{}

	removed bool
	url     string

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker         WorkerID
	acceptTaskType sealtasks.TaskType

	done chan *schedWindow

	groupID string
}

type schedWindow struct {
	groupID string
	//allocated activeResources

	// todo []*workerRequest
	todo *workerRequest
}

type workerDisableReq struct {
	//activeWindows []*schedWindow
	wid     WorkerID
	groupID string

	done func()
}

// type activeResources struct {
// 	// memUsedMin uint64
// 	// memUsedMax uint64
// 	// gpuUsed    bool
// 	// cpuUse     uint64
// 	P1  uint32
// 	P2  uint32
// 	C1  uint32
// 	C2  uint32
// 	AP  uint32
// 	FIN uint32

// 	cond *sync.Cond
// }

type workerRequest struct {
	sector   storage.SectorRef
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
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

func (sh *scheduler) loadEnv() {
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
		p1TicketsPerInterval = 0
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

func newScheduler() *scheduler {
	sch := &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		// schedQueue: &requestQueue{},

		p1GroupBuckets: make(map[string]*groupBuckets),

		openWindowGroups: make(map[string]*schedWindowRequestsGroup),
		openWindowsC2:    make([]*schedWindowRequest, 0, 16),

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}

	sch.loadEnv()
	log.Infof("newScheduler, p1TicketsPerInterval:%d, p1TicketInterval:%d, finTicketsPerInterval:%d, finTicketInterval:%d",
		sch.p1TicketsPerInterval, sch.p1TicketInterval, sch.finTicketsPerInterval, sch.finTicketInterval)
	return sch
}

func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	// mark
	sh.minerID = sector.ID.Miner

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
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

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (g *schedWindowRequestsGroup) addWindow(req *schedWindowRequest) {
	openWindows, _ := g.openWindows[req.acceptTaskType]
	openWindows = append(openWindows, req)
	g.openWindows[req.acceptTaskType] = openWindows
}

func (g *schedWindowRequestsGroup) addReq(req *workerRequest) {
	tt := req.taskType
	queue, _ := g.tasks[tt]
	queue = append(queue, req)
	g.tasks[tt] = queue
}

func (g *schedWindowRequestsGroup) removeByWorkerID(wid WorkerID) {
	for k, windowOfTT := range g.openWindows {
		openWindows := make([]*schedWindowRequest, 0, len(windowOfTT))
		for _, window := range windowOfTT {
			if window.worker != wid {
				openWindows = append(openWindows, window)
			}
		}

		g.openWindows[k] = openWindows
	}
}

func (sh *scheduler) acceptReqWindow(req *schedWindowRequest) {
	if req.acceptTaskType == sealtasks.TTCommit2 {
		if req.groupID != "" {
			log.Warnf("C2 worker should not use group tag:%s", req.groupID)
		}

		sh.openWindowsC2 = append(sh.openWindowsC2, req)
	} else {
		if req.groupID == "" {
			log.Errorf("non-C2 worker should use group tag:%s, task type:%s, workerID:%s",
				req.groupID, req.acceptTaskType, req.worker)
		} else {
			openWindowsGroup := sh.getOpenWindowsGroup(req.groupID)
			openWindowsGroup.addWindow(req)
		}
	}
}

func (sh *scheduler) getOpenWindowsGroup(groupID string) *schedWindowRequestsGroup {
	openWindowsGroup, _ := sh.openWindowGroups[groupID]
	if openWindowsGroup == nil {
		sh.openWindowGroups[groupID] = &schedWindowRequestsGroup{
			openWindows: make(map[sealtasks.TaskType][]*schedWindowRequest),
			tasks:       make(map[sealtasks.TaskType][]*workerRequest),
		}
		openWindowsGroup, _ = sh.openWindowGroups[groupID]
	}

	return openWindowsGroup
}

func (sh *scheduler) runSched() {
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
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			sh.acceptReq(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
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
					sh.acceptReq(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.acceptReqWindow(req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				// for _, window := range req.activeWindows {
				// 	// for _, request := range window.todo {
				// 	// 	sh.schedQueue.Push(request)
				// 	// }
				// 	if window.todo != nil {
				// 		sh.acceptReq(window.todo)
				// 	}
				// }

				// Non-C2
				groupID := req.groupID
				openWindowsGroup := sh.getOpenWindowsGroup(groupID)
				if openWindowsGroup != nil {
					openWindowsGroup.removeByWorkerID(req.wid)
				}

				// C2
				windowOfTT := sh.openWindowsC2
				openWindows := make([]*schedWindowRequest, 0, len(windowOfTT))
				for _, window := range windowOfTT {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindowsC2 = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *scheduler) acceptReq(req *workerRequest) {
	switch req.taskType {
	case sealtasks.TTAddPiece:
		// addpiece
		sh.addPieceQueue = append(sh.addPieceQueue, req)
	case sealtasks.TTCommit2:
		// c2
		sh.c2Queue = append(sh.c2Queue, req)
	default:
		// all group-specific task type
		groupID := req.sel.GroupID()
		if groupID == "" {
			log.Errorf("scheduler acceptReq failed: sector %v, task type:%s, group can't be empty",
				req.sector.ID, req.taskType)
		} else {
			// find group
			openWindowsGroup := sh.getOpenWindowsGroup(groupID)
			openWindowsGroup.addReq(req)
		}
	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	// lingh TODO: add all tasks
	for sqi := 0; sqi < len(sh.addPieceQueue); sqi++ {
		task := sh.addPieceQueue[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	// c2
	for sqi := 0; sqi < len(sh.c2Queue); sqi++ {
		task := sh.c2Queue[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, windowOfG := range sh.openWindowGroups {
		for _, windowOfTT := range windowOfG.openWindows {
			for _, window := range windowOfTT {
				out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
			}
		}
	}

	for _, window := range sh.openWindowsC2 {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySched() {
	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	// schedule AddPiece Task
	sh.trySchedAddPiece()

	// schedule Groups
	sh.trySchedGroups()

	// schedule C2 Task
	sh.trySchedC2()
}

func (sh *scheduler) trySchedQueue(queue []*workerRequest, dodo func(*workerRequest) bool) (int, []*workerRequest) {
	queuneLen := len(queue)
	if queuneLen < 1 {
		return 0, nil
	}

	hasDoneSched := 0
	for i := 0; i < queuneLen; i++ {
		schReq := queue[i]
		if dodo(schReq) {
			hasDoneSched++
		} else {
			break
		}
	}

	if hasDoneSched > 0 {
		n := copy(queue, queue[hasDoneSched:])
		queue = queue[0:n]

		return hasDoneSched, queue
	}

	return 0, nil
}

func (sh *scheduler) trySchedAddPiece() {
	queuneLen := len(sh.addPieceQueue)
	if queuneLen < 1 {
		return
	}

	log.Debugf("trySchedAddPiece begin, addPieceQueue len:%d", queuneLen)

	hasDoneSched, remainQueue := sh.trySchedQueue(sh.addPieceQueue, sh.schedOneAddPiece)
	if hasDoneSched > 0 {
		sh.addPieceQueue = remainQueue
	}

	log.Debugf("trySchedAddPiece completed, sched done:%d", hasDoneSched)
}

func (sh *scheduler) schedOneAddPiece(schReq *workerRequest) bool {
	taskType := schReq.taskType
	selector, ok := schReq.sel.(*addPieceSelector)

	if !ok {
		log.Error("schedOneAddPiece failed, selector not addPieceSelector")
		return false
	}

	best := selector.findBestStorages()
	if len(best) < 1 {
		log.Debugf("schedOneAddPiece sector %d, taskType:%s, no available storage group found",
			schReq.sector.ID.Number,
			taskType)
		return false
	}

	for _, store := range best {
		groupID := store.GroupID
		var openWindowsGroup *schedWindowRequestsGroup
		var openWindowsTT []*schedWindowRequest

		if groupID != "" {
			openWindowsGroup = sh.getOpenWindowsGroup(groupID)
			openWindowsTT, _ = openWindowsGroup.openWindows[schReq.taskType]
		} else {
			continue
		}

		done, remainWindows := sh.trySchedReq(schReq, groupID, openWindowsTT)
		if done {
			openWindowsGroup.openWindows[schReq.taskType] = remainWindows
			return true
		}
	}

	return false
}

func (sh *scheduler) trySchedC2() {
	queuneLen := len(sh.c2Queue)
	if queuneLen < 1 {
		return
	}

	log.Debugf("trySchedC2 begin, c2Queue len:%d", queuneLen)

	hasDoneSched, remainQueue := sh.trySchedQueue(sh.c2Queue, sh.schedOneC2)
	if hasDoneSched > 0 {
		sh.c2Queue = remainQueue
	}

	log.Debugf("trySchedC2 completed, sched done:%d", hasDoneSched)
}

func (sh *scheduler) schedOneC2(schReq *workerRequest) bool {
	var openWindowsTT []*schedWindowRequest
	openWindowsTT = sh.openWindowsC2

	done, remainWindows := sh.trySchedReq(schReq, "", openWindowsTT)
	if done {
		sh.openWindowsC2 = remainWindows
	}

	return done
}

func (sh *scheduler) trySchedGroups() {
	wg := &sync.WaitGroup{}
	wg.Add(len(sh.openWindowGroups))

	for groupID, openwindowGroup := range sh.openWindowGroups {
		gid := groupID
		openg := openwindowGroup

		go func() {
			tasks := openg.tasks
			update := make(map[sealtasks.TaskType][]*workerRequest)

			for tt, tarray := range tasks {
				log.Debugf("trySchedGroupTask begin, group %s, tasktype %s, queue len:%d",
					gid, tt, len(tarray))

				hasDoneSched, remainArray := sh.trySchedGroupTask(tt, gid,
					tarray, openg)

				log.Debugf("trySchedGroupTask completed, group %s, tasktype %s, done:%d",
					gid, tt, hasDoneSched)

				if hasDoneSched > 0 {
					update[tt] = remainArray
				}
			}

			for tt, tarray := range update {
				tasks[tt] = tarray
			}

			wg.Done()
		}()
	}

	wg.Wait()
}

func (sh *scheduler) trySchedGroupTask(tasktype sealtasks.TaskType,
	groupID string,
	tarray []*workerRequest,
	g *schedWindowRequestsGroup) (int, []*workerRequest) {

	remainWindows, _ := g.openWindows[tasktype]
	if len(remainWindows) < 1 {
		return 0, tarray
	}

	// find open windows to handle worker reqeust
	handled := 0
	hasDone := false
	for i := 0; i < len(tarray); i++ {
		req := tarray[i]
		hasDone, remainWindows = sh.trySchedReq(req, groupID, remainWindows)
		if !hasDone {
			break
		}
		handled++
	}

	if handled > 0 {
		g.openWindows[tasktype] = remainWindows
		// oh no!
		n := copy(tarray, tarray[handled:])
		tarray = tarray[0:n]
	}

	return handled, tarray
}

func (sh *scheduler) trySchedReq(schReq *workerRequest, groupID string,
	openWindowsTT []*schedWindowRequest) (result bool, windows []*schedWindowRequest) {

	taskType := schReq.taskType
	result = false
	windows = openWindowsTT

	if len(openWindowsTT) < 1 {
		log.Debugf("SCHED sector %d, taskType:%s, no available open window, group:%s",
			schReq.sector.ID.Number,
			taskType, groupID)

		return
	}

	// when failed to schedule request, free buckets
	var p1bucket *groupBuckets = nil
	var f1bucket *groupBuckets = nil
	defer func() {
		if !result {
			if p1bucket != nil {
				p1bucket.free()
			}

			if f1bucket != nil {
				f1bucket.free()
			}
		}
	}()

	if taskType == sealtasks.TTPreCommit1 {
		bucket, ok := sh.p1GroupBuckets[groupID]
		if ok {
			if !bucket.use() {
				log.Debugf("task acquire P1 ticket, sector:%d group:%s, no ticket remain",
					schReq.sector.ID.Number,
					groupID)
				return
			}

			p1bucket = bucket
			log.Debugf("task acquire P1 ticket, sector:%d group:%s, remain:%d",
				schReq.sector.ID.Number,
				groupID, bucket.atomicTikets)
		}
	}

	if taskType == sealtasks.TTFinalize && sh.finTicketInterval > 0 {
		if !sh.finTickets.use() {
			log.Debugf("task acquire Finalize ticket, sector:%d group:%s, no ticket remain",
				schReq.sector.ID.Number,
				groupID)

			return
		}

		f1bucket = sh.finTickets
		log.Debugf("task acquire Finalize ticket, sector:%d group:%s, remain:%d",
			schReq.sector.ID.Number,
			groupID, f1bucket.atomicTikets)
	}

	for wnd, windowRequest := range openWindowsTT {
		worker, ok := sh.workers[windowRequest.worker]
		if !ok {
			log.Errorf("worker referenced by windowRequest not found (worker: %s)",
				windowRequest.worker)
			// TODO: How to move forward here?
			continue
		}

		if !worker.enabled {
			log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
			continue
		}

		if _, paused := worker.paused[taskType]; paused {
			log.Debugw("skipping paused worker", "worker", windowRequest.worker)
			continue
		}

		if taskType == sealtasks.TTCommit2 && windowRequest.groupID != "" {
			// check if group has P2, then P2 first
			g, kk := sh.openWindowGroups[windowRequest.groupID]
			if kk {
				p2Tasks, hasP2 := g.tasks[sealtasks.TTPreCommit2]
				if hasP2 && len(p2Tasks) > 0 {
					log.Debugf("C2 skipping worker that has P2 task, group:%s", windowRequest.groupID)
					continue
				}
			}
		}

		rpcCtx, cancel := context.WithTimeout(schReq.ctx, SelectorTimeout)
		ok, err := schReq.sel.Ok(rpcCtx, taskType, schReq.sector.ProofType, worker)
		cancel()
		if err != nil {
			log.Errorf("trySched(1) sector:%d, group:%s, task-type:%s, req.sel.Ok error: %+v",
				schReq.sector.ID.Number,
				windowRequest.groupID, taskType, err)
			continue
		}

		if !ok {
			// selector not allow
			continue
		}

		log.Debugf("SCHED assign sector %d to window %d, group:%s, task-type:%s",
			schReq.sector.ID.Number,
			wnd, windowRequest.groupID, schReq.taskType)

		window := schedWindow{
			todo:    schReq,
			groupID: windowRequest.groupID,
		}

		select {
		case windowRequest.done <- &window:
		default:
			log.Errorf("expected sh.openWindows[wnd].done to be buffered, sector %d to window %d, group:%s, task-type:%s",
				schReq.sector.ID.Number,
				wnd, windowRequest.groupID, schReq.taskType)
			// found next available window
			continue
		}

		// done, remove that open window
		l := len(openWindowsTT)
		openWindowsTT[l-1], openWindowsTT[wnd] = openWindowsTT[wnd], openWindowsTT[l-1]
		// update windows
		result = true
		windows = openWindowsTT[0:(l - 1)]
		return
	}

	return
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
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

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (sh *scheduler) doMamami() {
	// miner id
	minerID := uint64(sh.minerID)
	// worker count
	workerCount := len(sh.workers)

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
