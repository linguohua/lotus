package sealer

import (
	"context"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/storage/paths"
	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type schedWorker struct {
	sched  *Scheduler
	worker *WorkerHandle

	wid storiface.WorkerID

	heartbeatTimer   *time.Ticker
	scheduledWindows chan *SchedWindow
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

func newWorkerHandle(ctx context.Context, w Worker, url string) (*WorkerHandle, error) {
	info, err := w.Info(ctx)
	if err != nil {
		return nil, xerrors.Errorf("getting worker info: %w", err)
	}

	acceptTaskTypes, taskTypeCounters := info.Resources.ValidTaskType()
	worker := &WorkerHandle{
		workerRpc: w,
		Info:      info,

		acceptTaskTypes:  acceptTaskTypes,
		taskTypeCounters: taskTypeCounters,

		//preparing: &activeResources{},
		//active:  &activeResources{},
		Enabled: true,
		paused:  make(map[sealtasks.TaskType]struct{}),
		url:     url,

		closingMgr: make(chan struct{}),
		closedMgr:  make(chan struct{}),

		windowCounters: make(map[sealtasks.TaskType]int),
	}

	return worker, nil
}

// context only used for startup
func (sh *Scheduler) runWorker(ctx context.Context, wid storiface.WorkerID, worker *WorkerHandle) error {
	sh.workersLk.Lock()
	_, exist := sh.Workers[wid]
	if exist {
		log.Warnw("duplicated worker added", "id", wid)

		// this is ok, we're already handling this worker in a different goroutine
		sh.workersLk.Unlock()
		return nil
	}

	sh.Workers[wid] = worker
	info := worker.Info
	acceptTaskTypes := worker.acceptTaskTypes

	if info.GroupID != "" && hasTaskType(acceptTaskTypes, sealtasks.TTPreCommit1) {
		_, exist = sh.p1GroupBuckets[info.GroupID]
		if !exist {
			gb := &groupBuckets{}
			gb.set(sh.p1TicketsPerInterval)
			sh.p1GroupBuckets[info.GroupID] = gb
		}
	}

	sh.workersLk.Unlock()

	schedWindowsCount := worker.Info.Resources.Windows()

	sw := &schedWorker{
		sched:  sh,
		worker: worker,

		wid: wid,

		//scheduledWindows: make(chan *SchedWindow, SchedWindows),
		heartbeatTimer:   time.NewTicker(paths.HeartbeatInterval),
		scheduledWindows: make(chan *SchedWindow, schedWindowsCount),

		taskDone: make(chan struct{}, 1),
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

func (sw *WorkerHandle) pauseStat() string {
	str := ""
	for k := range sw.paused {
		str = str + string(k) + ","
	}

	return str
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
		delete(sched.Workers, sw.wid)
		sched.workersLk.Unlock()
	}()

	defer sw.heartbeatTimer.Stop()

	for {
		{
			sched.workersLk.Lock()
			enabled := worker.Enabled
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
				enabled := worker.Enabled
				worker.Enabled = true
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
		groupID: sw.worker.Info.GroupID,
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
		sctx, scancel := context.WithTimeout(ctx, paths.HeartbeatInterval/2)
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

		if storiface.WorkerID(curSes) != sw.wid {
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
		case sw.sched.windowRequests <- &SchedWindowRequest{
			acceptTaskType: taskType,
			Worker:         sw.wid,
			Done:           sw.scheduledWindows,
			groupID:        sw.worker.Info.GroupID,
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

	//		for ti, todo := range window.Todo {
	//			needRes := worker.Info.Resources.ResourceSpec(todo.Sector.ProofType, todo.TaskType)
	//			if !lower.Allocated.CanHandleRequest(todo.SealTask(), needRes, sw.wid, "compactWindows", worker.Info) {
	//				continue
	//			}
	//
	//			moved = append(moved, ti)
	//			lower.Todo = append(lower.Todo, todo)
	//			lower.Allocated.Add(todo.SealTask(), worker.Info.Resources, needRes)
	//			window.Allocated.Free(todo.SealTask(), worker.Info.Resources, needRes)
	//		}
	//
	//		if len(moved) > 0 {
	//			newTodo := make([]*WorkerRequest, 0, len(window.Todo)-len(moved))
	//			for i, t := range window.Todo {
	//				if len(moved) > 0 && moved[0] == i {
	//					moved = moved[1:]
	//					continue
	//				}
	//
	//				newTodo = append(newTodo, t)
	//			}
	//			window.Todo = newTodo
	//		}
	//	}
	//}
	//
	//var compacted int
	//var newWindows []*SchedWindow
	//
	//for _, window := range worker.activeWindows {
	//	if len(window.Todo) == 0 {
	//		compacted++
	//		continue
	//	}

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
		//for len(firstWindow.Todo) > 0 {
		//	tidx := -1
		//
		//	worker.lk.Lock()
		//	for t, todo := range firstWindow.Todo {
		//		needRes := worker.Info.Resources.ResourceSpec(todo.Sector.ProofType, todo.TaskType)
		//		if worker.preparing.CanHandleRequest(todo.SealTask(), needRes, sw.wid, "startPreparing", worker.Info) {
		//			tidx = t
		//			break
		//		}
		//	}
		//	worker.lk.Unlock()

		//worker.lk.Lock()
		//for t, todo := range firstWindow.todo {
		//needRes := ResourceTable[todo.taskType][todo.sector.ProofType]
		//if worker.preparing.canHandleRequest(needRes, sw.wid, "startPreparing", worker.info.Resources) {
		//tidx = t
		//break
		//}
		//}
		//worker.lk.Unlock()

		//	todo := firstWindow.Todo[tidx]
		//
		//	log.Debugf("assign worker sector %d", todo.Sector.ID.Number)
		//	err := sw.startProcessingTask(todo)
		//
		//	if err != nil {
		//		log.Errorf("startProcessingTask error: %+v", err)
		//		go todo.respond(xerrors.Errorf("startProcessingTask error: %w", err))
		//	}
		//
		//	// Note: we're not freeing window.allocated resources here very much on purpose
		//	copy(firstWindow.Todo[tidx:], firstWindow.Todo[tidx+1:])
		//	firstWindow.Todo[len(firstWindow.Todo)-1] = nil
		//	firstWindow.Todo = firstWindow.Todo[:len(firstWindow.Todo)-1]
		// if tidx == -1 {
		// 	break assignLoop
		// }

		// todo := firstWindow.todo[tidx]
		todo := firstWindow.Todo
		log.Debugf("assign worker sector %d", todo.Sector.ID.Number)
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

func (sw *schedWorker) startProcessingTask(taskDone chan struct{}, window *SchedWindow) error {
	w, sh := sw.worker, sw.sched

	// needRes := ResourceTable[req.taskType][req.sector.ProofType]

	// w.lk.Lock()
	// w.preparing.add(w.info.Resources, needRes)
	// w.lk.Unlock()

	go func() {
		// first run the prepare step (e.g. fetching sector data from other worker)
		req := window.Todo
		//log.Debugf("startProcessingTask call prepare %d", req.sector.ID.Number)
		tw := sh.workTracker.worker(sw.wid, w.Info, w.workerRpc)
		tw.start()
		err := req.prepare(req.Ctx, tw)
		//sh.workersLk.Lock()

		if err != nil {
			// w.lk.Lock()
			// w.preparing.free(w.info.Resources, needRes)
			// w.lk.Unlock()
			// sh.workersLk.Unlock()
			// release window
			sw.releaseWindowOfTasktype(window.Todo.TaskType)

			select {
			case taskDone <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.Ctx.Done():
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
			tw := sh.workTracker.worker(sw.wid, w.Info, w.workerRpc)
			tw.start()
			err = req.work(req.Ctx, tw)

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.Ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			sw.releaseWindowOfTasktype(window.Todo.TaskType)
			select {
			// notify request window
			case taskDone <- struct{}{}:
			case <-sh.closing:
			}
			return nil
		}

		//log.Debugf("startProcessingTask call dowork %d", req.sector.ID.Number)
		err = dowork()
		//sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}
