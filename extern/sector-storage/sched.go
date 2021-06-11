package sectorstorage

import (
	"context"
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
var SelectorTimeout = 5 * time.Second
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
	groupID string
	tikets  int
}

type groupSchedWindowRequests struct {
	openWindows map[sealtasks.TaskType][]*schedWindowRequest
}

type scheduler struct {
	workersLk            sync.RWMutex
	workers              map[WorkerID]*workerHandle
	p1GroupBuckets       map[string]*groupBuckets
	p1TicketsPerInterval int
	p1TicketInterval     int

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue *requestQueue

	openWindowsByGroup map[string]*groupSchedWindowRequests
	openWindowsC2      []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	acceptTaskTypes     []sealtasks.TaskType
	taskTypeValidcounts []uint32

	//preparing *activeResources
	active *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow
	// count all request windows
	requestedWindowsCounter map[sealtasks.TaskType]int

	paused bool
	url    string

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
	activeWindows []*schedWindow
	wid           WorkerID
	groupID       string

	done func()
}

type activeResources struct {
	// memUsedMin uint64
	// memUsedMax uint64
	// gpuUsed    bool
	// cpuUse     uint64
	P1  uint32
	P2  uint32
	C1  uint32
	C2  uint32
	AP  uint32
	FIN uint32

	cond *sync.Cond
}

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

func newScheduler() *scheduler {
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

	log.Infof("newScheduler, p1TicketsPerInterval:%d, p1TicketInterval:%d", p1TicketsPerInterval, p1TicketInterval)
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		p1GroupBuckets:       make(map[string]*groupBuckets),
		p1TicketsPerInterval: p1TicketsPerInterval,
		p1TicketInterval:     p1TicketInterval,

		openWindowsByGroup: make(map[string]*groupSchedWindowRequests),
		openWindowsC2:      make([]*schedWindowRequest, 0, 16),

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

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

func (g *groupSchedWindowRequests) addWindow(req *schedWindowRequest) {
	openWindows, _ := g.openWindows[req.acceptTaskType]
	openWindows = append(openWindows, req)
	g.openWindows[req.acceptTaskType] = openWindows
}

func (g *groupSchedWindowRequests) removeByWorkerID(wid WorkerID) {
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
			openWindowsOfGroup, _ := sh.openWindowsByGroup[req.groupID]
			if openWindowsOfGroup == nil {
				sh.openWindowsByGroup[req.groupID] = &groupSchedWindowRequests{
					openWindows: make(map[sealtasks.TaskType][]*schedWindowRequest),
				}
				openWindowsOfGroup, _ = sh.openWindowsByGroup[req.groupID]
			}

			openWindowsOfGroup.addWindow(req)
		}
	}
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)

	go func() {
		ticker := time.NewTicker(time.Duration(sh.p1TicketInterval) * time.Minute)
		for {
			select {
			case <-ticker.C:
				sh.workersLk.Lock()
				changed := false
				for _, g := range sh.p1GroupBuckets {
					if g.tikets < sh.p1TicketsPerInterval {
						g.tikets = sh.p1TicketsPerInterval
						changed = true
						log.Infof("reset group %s P1 tickets to %d", g.groupID, g.tikets)
					}
				}
				sh.workersLk.Unlock()
				if changed {
					sh.workerChange <- struct{}{}
				}
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
			sh.schedQueue.Push(req)
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
					sh.schedQueue.Push(req)
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
				for _, window := range req.activeWindows {
					// for _, request := range window.todo {
					// 	sh.schedQueue.Push(request)
					// }
					if window.todo != nil {
						sh.schedQueue.Push(window.todo)
					}
				}

				groupID := req.groupID
				if groupID != "" {
					openWindowsGroup, _ := sh.openWindowsByGroup[groupID]
					if openWindowsGroup != nil {
						openWindowsGroup.removeByWorkerID(req.wid)
					}
				} else {
					windowOfTT := sh.openWindowsC2
					openWindows := make([]*schedWindowRequest, 0, len(windowOfTT))
					for _, window := range windowOfTT {
						if window.worker != req.wid {
							openWindows = append(openWindows, window)
						}
					}
					sh.openWindowsC2 = openWindows
				}

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, windowOfG := range sh.openWindowsByGroup {
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
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()
	queuneLen := sh.schedQueue.Len()
	log.Debugf("trySched begin, queue len:%d", queuneLen)

	hasDoneSched := make([]int, 0, queuneLen)
	for i := 0; i < queuneLen; i++ {
		schReq := (*sh.schedQueue)[i]
		if sh.schedOne(schReq) {
			hasDoneSched = append(hasDoneSched, i)
		}
	}

	if len(hasDoneSched) > 0 {
		for i := len(hasDoneSched) - 1; i >= 0; i-- {
			sh.schedQueue.Remove(hasDoneSched[i])
		}
	}

	log.Debugf("trySched completed, sched done:%d", len(hasDoneSched))
}

func (sh *scheduler) schedOne(schReq *workerRequest) bool {
	taskType := schReq.taskType
	groupID := schReq.sel.GroupID()

	var openWindowsOfG *groupSchedWindowRequests
	var openWindowsTT []*schedWindowRequest
	if groupID != "" {
		log.Debugf("schedOne sector %d, task type:%s, groupID:%s use group-openwindows",
			schReq.sector.ID.Number, taskType, groupID)

		if taskType == sealtasks.TTCommit2 || taskType == sealtasks.TTAddPiece {
			log.Errorf("schedOne sector %d, task type:%s, groupID:%s C2/addPiece task should not use group-openwindows",
				schReq.sector.ID.Number, taskType, groupID)
		}

		openWindowsOfG, _ = sh.openWindowsByGroup[groupID]
		if openWindowsOfG != nil {
			openWindowsTT, _ = openWindowsOfG.openWindows[schReq.taskType]
		}
	} else {
		if taskType == sealtasks.TTCommit2 {
			openWindowsTT = sh.openWindowsC2
			log.Debugf("schedOne sector %d, task type:%s, groupID:%s use c2-openwindows",
				schReq.sector.ID.Number, taskType, groupID)

			if taskType != sealtasks.TTCommit2 {
				log.Errorf("schedOne sector %d, task type:%s, groupID:%s non-C2 task should not use C2-openwindows",
					schReq.sector.ID.Number, taskType, groupID)
			}
		} else if taskType == sealtasks.TTAddPiece {
			for _, v := range sh.openWindowsByGroup {
				tt, ok := v.openWindows[taskType]
				if ok && len(tt) > 0 {
					openWindowsOfG = v
					openWindowsTT = tt
					break
				}
			}
		} else {
			log.Errorf("schedOne sector %d, task type:%s, non-group-task only support addPiece/C2",
				schReq.sector.ID.Number, taskType, groupID)
		}
	}

	if len(openWindowsTT) < 1 {
		log.Debugf("SCHED sector %d, taskType:%s, no available open window, group:%s", schReq.sector.ID.Number,
			taskType, groupID)

		return false
	}

	for wnd, windowRequest := range openWindowsTT {
		worker, ok := sh.workers[windowRequest.worker]
		if !ok {
			log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
			// TODO: How to move forward here?
			continue
		}

		if !worker.enabled {
			log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
			continue
		}

		if worker.paused {
			log.Debugw("skipping paused worker", "worker", windowRequest.worker)
			continue
		}

		rpcCtx, cancel := context.WithTimeout(schReq.ctx, SelectorTimeout)
		ok, err := schReq.sel.Ok(rpcCtx, schReq.taskType, schReq.sector.ProofType, worker)
		cancel()
		if err != nil {
			log.Errorf("trySched(1) sector:%d, group:%s, task-type:%s, req.sel.Ok error: %+v", schReq.sector.ID.Number,
				windowRequest.groupID, schReq.taskType, err)
			continue
		}

		if !ok {
			// selector not allow
			continue
		}

		if schReq.taskType == sealtasks.TTPreCommit1 {
			groupID := windowRequest.groupID
			bucket, ok := sh.p1GroupBuckets[groupID]
			if ok {
				if bucket.tikets < 1 {
					log.Debugf("task acquire P1 ticket, sector:%d group:%s, no ticket remain", schReq.sector.ID.Number,
						groupID)
					continue
				}

				bucket.tikets--
				log.Debugf("task acquire P1 ticket, sector:%d group:%s, remain:%d", schReq.sector.ID.Number,
					groupID, bucket.tikets)
			}
		}

		log.Debugf("SCHED assign sector %d to window %d, group:%s, task-type:%s", schReq.sector.ID.Number,
			wnd, windowRequest.groupID, schReq.taskType)

		window := schedWindow{
			todo:    schReq,
			groupID: windowRequest.groupID,
		}

		select {
		case windowRequest.done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
			// found next available window
			continue
		}

		// done, remove that open window
		l := len(openWindowsTT)
		openWindowsTT[l-1], openWindowsTT[wnd] = openWindowsTT[wnd], openWindowsTT[l-1]
		// update windows
		if openWindowsOfG != nil {
			openWindowsOfG.openWindows[schReq.taskType] = openWindowsTT[0:(l - 1)]
		} else {
			sh.openWindowsC2 = openWindowsTT[0:(l - 1)]
		}

		return true
	}

	return false
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
