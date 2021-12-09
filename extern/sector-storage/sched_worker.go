package sectorstorage

import (
	"context"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/google/uuid"
)

type schedWorker struct {
	sched  *scheduler
	worker *workerHandle

	wid WorkerID

	heartbeatTimer   *time.Ticker
	scheduledWindows chan *schedWindow
	taskDone         chan struct{}
}

func hasTaskType(acceptTaskTypes []sealtasks.TaskType, target sealtasks.TaskType) bool {
	for _, r := range acceptTaskTypes {
		if r == target {
			return true
		}
	}
	return false
}

// context only used for startup
func (sh *scheduler) runWorker(ctx context.Context, w Worker, url string) error {
	info, err := w.Info(ctx)
	if err != nil {
		return xerrors.Errorf("getting worker info: %w", err)
	}

	sessID, err := w.Session(ctx)
	if err != nil {
		return xerrors.Errorf("getting worker session: %w", err)
	}
	if sessID == ClosedWorkerID {
		return xerrors.Errorf("worker already closed")
	}

	acceptTaskTypes, taskTypeCounters := info.Resources.ValidTaskType()
	worker := &workerHandle{
		workerRpc: w,
		info:      info,

		acceptTaskTypes:  acceptTaskTypes,
		taskTypeCounters: taskTypeCounters,

		//preparing: &activeResources{},
		//active:  &activeResources{},
		enabled: true,
		paused:  make(map[sealtasks.TaskType]struct{}),
		url:     url,

		closingMgr: make(chan struct{}),
		closedMgr:  make(chan struct{}),

		windowCounters: make(map[sealtasks.TaskType]int),
	}

	wid := WorkerID(sessID)

	sh.workersLk.Lock()
	_, exist := sh.workers[wid]
	if exist {
		log.Warnw("duplicated worker added", "id", wid)

		// this is ok, we're already handling this worker in a different goroutine
		sh.workersLk.Unlock()
		return nil
	}

	sh.workers[wid] = worker

	if info.GroupID != "" && hasTaskType(acceptTaskTypes, sealtasks.TTPreCommit1) {
		_, exist = sh.p1GroupBuckets[info.GroupID]
		if !exist {
			gb := &groupBuckets{}
			gb.set(sh.p1TicketsPerInterval)
			sh.p1GroupBuckets[info.GroupID] = gb
		}
	}

	sh.workersLk.Unlock()

	schedWindowsCount := worker.info.Resources.Windows()

	sw := &schedWorker{
		sched:  sh,
		worker: worker,

		wid: wid,

		heartbeatTimer:   time.NewTicker(stores.HeartbeatInterval),
		scheduledWindows: make(chan *schedWindow, schedWindowsCount),
		taskDone:         make(chan struct{}, 1),
	}

	log.Infof("schedWorker.runWorker, group id:%s, host name:%s", info.GroupID, info.Hostname)

	go sw.handleWorker()

	return nil
}

func convertTaskTypes(tt string) []sealtasks.TaskType {
	switch tt {
	case "ap":
		return []sealtasks.TaskType{sealtasks.TTAddPiece}
	case "p1":
		return []sealtasks.TaskType{sealtasks.TTPreCommit1}
	case "p2":
		return []sealtasks.TaskType{sealtasks.TTPreCommit2}
	case "c1":
		return []sealtasks.TaskType{sealtasks.TTCommit1}
	case "c2":
		return []sealtasks.TaskType{sealtasks.TTCommit2}
	case "fin":
		return []sealtasks.TaskType{sealtasks.TTFinalize}
	case "all":
		return []sealtasks.TaskType{sealtasks.TTAddPiece,
			sealtasks.TTPreCommit1, sealtasks.TTPreCommit2,
			sealtasks.TTCommit1, sealtasks.TTCommit2, sealtasks.TTFinalize}
	default:
		return []sealtasks.TaskType{}
	}
}

func (sh *scheduler) pauseWorker(ctx context.Context, uuid2 string, paused bool, tasktype string) error {
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
	worker, exist := sh.workers[WorkerID(wid)]
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

func (sw *workerHandle) pauseStat() string {
	str := ""
	for k := range sw.paused {
		str = str + string(k) + ","
	}

	return str
}

func (sh *scheduler) removeWorker(ctx context.Context, uuid2 string) error {
	log.Infof("removeWorker call with uuid:%s, removed: %v", uuid2)
	wid, err := uuid.Parse(uuid2)
	if err != nil {
		return xerrors.Errorf("removeWorker failed: parse uuid %s error %v", uuid2, err)
	}

	log.Infof("removeWorker call wait RLock")
	sh.workersLk.RLock()
	worker, exist := sh.workers[WorkerID(wid)]
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

func (sh *scheduler) updateFinalizeTicketsParams(ctx context.Context, tickets uint, interval uint) error {
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

func (sh *scheduler) updateP1TicketsParams(ctx context.Context, tickets uint, interval uint) error {
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

func (sw *schedWorker) handleWorker() {
	worker, sched := sw.worker, sw.sched

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	defer close(worker.closedMgr)

	defer func() {
		log.Warnw("Worker closing", "workerid", sw.wid)

		if err := sw.disable(ctx); err != nil {
			log.Warnw("failed to disable worker", "worker", sw.wid, "error", err)
		}

		sched.workersLk.Lock()
		delete(sched.workers, sw.wid)
		sched.workersLk.Unlock()
	}()

	defer sw.heartbeatTimer.Stop()

	for {
		{
			sched.workersLk.Lock()
			enabled := worker.enabled
			sched.workersLk.Unlock()

			// ask for more windows if we need them (non-blocking)
			if enabled {
				if !sw.fillWindows() {
					return // graceful shutdown
				}
			}
		}

		// wait for more windows to come in, or for tasks to get finished (blocking)
		for {
			// ping the worker and check session
			if !sw.checkSession(ctx) {
				return // invalid session / exiting
			}

			// session looks good
			{
				sched.workersLk.Lock()
				enabled := worker.enabled
				worker.enabled = true
				sched.workersLk.Unlock()

				if !enabled {
					// go send window requests
					break
				}
			}

			// wait for more tasks to be assigned by the main scheduler or for the worker
			// to finish precessing a task
			update, pokeSched, ok := sw.waitForUpdates()
			if !ok {
				return
			}
			if pokeSched {
				// a task has finished preparing, which can mean that we've freed some space on some worker
				select {
				case sched.workerChange <- struct{}{}:
				default: // workerChange is buffered, and scheduling is global, so it's ok if we don't send here
				}
			}
			if update {
				break
			}
		}

		// process assigned windows (non-blocking)
		sched.workersLk.RLock()
		worker.wndLk.Lock()

		sw.workerCompactWindows()

		// send tasks to the worker
		sw.processAssignedWindows()

		worker.wndLk.Unlock()
		sched.workersLk.RUnlock()
	}
}

func (sw *schedWorker) disable(ctx context.Context) error {
	done := make(chan struct{})

	// request cleanup in the main scheduler goroutine
	select {
	case sw.sched.workerDisable <- workerDisableReq{
		//activeWindows: sw.worker.activeWindows,
		wid:     sw.wid,
		groupID: sw.worker.info.GroupID,
		done: func() {
			close(done)
		},
	}:
	case <-ctx.Done():
		return ctx.Err()
	case <-sw.sched.closing:
		return nil
	}

	// wait for cleanup to complete
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	case <-sw.sched.closing:
		return nil
	}

	sw.worker.activeWindows = sw.worker.activeWindows[:0]
	sw.worker.windowCounters = make(map[sealtasks.TaskType]int)

	return nil
}

func (sw *schedWorker) checkSession(ctx context.Context) bool {
	for {
		sctx, scancel := context.WithTimeout(ctx, stores.HeartbeatInterval/2)
		curSes, err := sw.worker.workerRpc.Session(sctx)
		scancel()
		if err != nil {
			// Likely temporary error

			log.Warnw("failed to check worker session", "error", err)

			if err := sw.disable(ctx); err != nil {
				log.Warnw("failed to disable worker with session error", "worker", sw.wid, "error", err)
			}

			if sw.worker.removed {
				log.Warnf("failed to check worker session and worker %s is removed by user, exit schedule goroutine",
					sw.worker.url)
				return false
			}

			select {
			case <-sw.heartbeatTimer.C:
				continue
			case w := <-sw.scheduledWindows:
				// was in flight when initially disabled, return
				sw.worker.wndLk.Lock()
				sw.worker.activeWindows = append(sw.worker.activeWindows, w)
				sw.worker.wndLk.Unlock()

				if err := sw.disable(ctx); err != nil {
					log.Warnw("failed to disable worker with session error", "worker", sw.wid, "error", err)
				}
			case <-sw.sched.closing:
				return false
			case <-sw.worker.closingMgr:
				return false
			}
			continue
		}

		if WorkerID(curSes) != sw.wid {
			if curSes != ClosedWorkerID {
				// worker restarted
				log.Warnw("worker session changed (worker restarted?)", "initial", sw.wid, "current", curSes)
			}

			return false
		}

		return true
	}
}

func (sw *schedWorker) fillWindowsByTasktype(taskType sealtasks.TaskType, i int) bool {
	log.Infof("schedWorker.fillWindowsByTasktype task type:%s, window count:%d, worker url:%s", taskType, i, sw.worker.url)
	for idx := 0; idx < i; idx++ {
		select {
		case sw.sched.windowRequests <- &schedWindowRequest{
			acceptTaskType: taskType,
			worker:         sw.wid,
			done:           sw.scheduledWindows,
			groupID:        sw.worker.info.GroupID,
		}:
		case <-sw.sched.closing:
			return false
		case <-sw.worker.closingMgr:
			return false
		}
	}
	return true
}

func (sw *schedWorker) releaseWindowOfTasktype(taskType sealtasks.TaskType) {
	sw.worker.wndLk.Lock()
	count, _ := sw.worker.windowCounters[taskType]
	count = count - 1
	if count < 0 {
		count = 0
	}
	sw.worker.windowCounters[taskType] = count
	sw.worker.wndLk.Unlock()
}

func (sw *schedWorker) fillWindows() bool {
	// acceptTaskType, validcounts := sw.worker.info.Resources.ValidTaskType()
	for i, t := range sw.worker.acceptTaskTypes {
		x := sw.worker.taskTypeCounters[i]

		sw.worker.wndLk.Lock()
		count, _ := sw.worker.windowCounters[t]
		diff := int(x) - count
		sw.worker.windowCounters[t] = count + diff
		sw.worker.wndLk.Unlock()

		if diff > 0 {
			b := sw.fillWindowsByTasktype(t, diff)
			if !b {
				return false
			}
		}
	}

	return true
}

func (sw *schedWorker) waitForUpdates() (update bool, sched bool, ok bool) {
	select {
	case <-sw.heartbeatTimer.C:
		return false, false, true
	case w := <-sw.scheduledWindows:
		sw.worker.wndLk.Lock()
		sw.worker.activeWindows = append(sw.worker.activeWindows, w)
		sw.worker.wndLk.Unlock()
		return true, false, true
	case <-sw.taskDone:
		log.Debugw("task done", "workerid", sw.wid)
		return true, true, true
	case <-sw.sched.closing:
	case <-sw.worker.closingMgr:
	}

	return false, false, false
}

func (sw *schedWorker) workerCompactWindows() {

	// worker := sw.worker

	// // move tasks from older windows to newer windows if older windows
	// // still can fit them
	// if len(worker.activeWindows) > 1 {
	// 	for wi, window := range worker.activeWindows[1:] {
	// 		lower := worker.activeWindows[wi]
	// 		var moved []int

	// 		for ti, todo := range window.todo {
	// 			needRes := ResourceTable[todo.taskType][todo.sector.ProofType]
	// 			if !lower.allocated.canHandleRequest(needRes, sw.wid, "compactWindows", worker.info.Resources) {
	// 				continue
	// 			}

	// 			moved = append(moved, ti)
	// 			lower.todo = append(lower.todo, todo)
	// 			lower.allocated.add(worker.info.Resources, needRes)
	// 			window.allocated.free(worker.info.Resources, needRes)
	// 		}

	// 		if len(moved) > 0 {
	// 			newTodo := make([]*workerRequest, 0, len(window.todo)-len(moved))
	// 			for i, t := range window.todo {
	// 				if len(moved) > 0 && moved[0] == i {
	// 					moved = moved[1:]
	// 					continue
	// 				}

	// 				newTodo = append(newTodo, t)
	// 			}
	// 			window.todo = newTodo
	// 		}
	// 	}
	// }

	// var compacted int
	// var newWindows []*schedWindow

	// for _, window := range worker.activeWindows {
	// 	if len(window.todo) == 0 {
	// 		compacted++
	// 		continue
	// 	}

	// 	newWindows = append(newWindows, window)
	// }

	// worker.activeWindows = newWindows
	// sw.windowsRequested -= compacted
}

func (sw *schedWorker) processAssignedWindows() {
	worker := sw.worker

	// assignLoop:
	// process windows in order
	for len(worker.activeWindows) > 0 {
		firstWindow := worker.activeWindows[0]

		// process tasks within a window, preferring tasks at lower indexes

		//for len(firstWindow.todo) > 0 {
		//for len(firstWindow.todo) > 0 {
		//tidx := -1

		//worker.lk.Lock()
		//for t, todo := range firstWindow.todo {
		//needRes := ResourceTable[todo.taskType][todo.sector.ProofType]
		//if worker.preparing.canHandleRequest(needRes, sw.wid, "startPreparing", worker.info.Resources) {
		//tidx = t
		//break
		//}
		//}
		//worker.lk.Unlock()

		// if tidx == -1 {
		// 	break assignLoop
		// }

		// todo := firstWindow.todo[tidx]
		todo := firstWindow.todo
		log.Debugf("assign worker sector %d", todo.sector.ID.Number)
		err := sw.startProcessingTask(sw.taskDone, firstWindow)

		if err != nil {
			log.Errorf("startProcessingTask error: %+v", err)
			go todo.respond(xerrors.Errorf("startProcessingTask error: %w", err))
		}

		// Note: we're not freeing window.allocated resources here very much on purpose
		//copy(firstWindow.todo[tidx:], firstWindow.todo[tidx+1:])
		//firstWindow.todo[len(firstWindow.todo)-1] = nil
		// firstWindow.todo = nil // firstWindow.todo[:len(firstWindow.todo)-1]
		//

		copy(worker.activeWindows, worker.activeWindows[1:])
		worker.activeWindows[len(worker.activeWindows)-1] = nil
		worker.activeWindows = worker.activeWindows[:len(worker.activeWindows)-1]

		//sw.windowsRequested--
	}
}

func (sw *schedWorker) startProcessingTask(taskDone chan struct{}, window *schedWindow) error {
	w, sh := sw.worker, sw.sched

	// needRes := ResourceTable[req.taskType][req.sector.ProofType]

	// w.lk.Lock()
	// w.preparing.add(w.info.Resources, needRes)
	// w.lk.Unlock()

	go func() {
		// first run the prepare step (e.g. fetching sector data from other worker)
		req := window.todo
		log.Debugf("startProcessingTask call prepare %d", req.sector.ID.Number)
		err := req.prepare(req.ctx, sh.workTracker.worker(sw.wid, w.info, w.workerRpc))
		//sh.workersLk.Lock()

		if err != nil {
			// w.lk.Lock()
			// w.preparing.free(w.info.Resources, needRes)
			// w.lk.Unlock()
			// sh.workersLk.Unlock()
			// release window
			sw.releaseWindowOfTasktype(window.todo.taskType)

			select {
			case taskDone <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		// wait (if needed) for resources in the 'active' window
		// err = w.active.withResources(sw.wid, w.info.Resources, req.taskType, &sh.workersLk, )

		dowork := func() error {
			// Do the work!
			err = req.work(req.ctx, sh.workTracker.worker(sw.wid, w.info, w.workerRpc))

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			sw.releaseWindowOfTasktype(window.todo.taskType)
			select {
			// notify request window
			case taskDone <- struct{}{}:
			case <-sh.closing:
			}
			return nil
		}

		log.Debugf("startProcessingTask call dowork %d", req.sector.ID.Number)
		err = dowork()
		//sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

func (sh *scheduler) workerCleanup(wid WorkerID, w *workerHandle) {
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

		groupID := w.info.GroupID
		if groupID != "" {
			openWindowsG := sh.getOpenWindowsGroup(groupID)
			if openWindowsG != nil {
				openWindowsG.removeByWorkerID(wid)
			}
		} else {
			openWindows := sh.openWindowsC2
			newWindows := make([]*schedWindowRequest, 0, len(openWindows))
			for _, window := range openWindows {
				if window.worker != wid {
					newWindows = append(newWindows, window)
				}
			}
			sh.openWindowsC2 = newWindows
		}

		log.Debugf("worker %s dropped", wid)
	}
}
