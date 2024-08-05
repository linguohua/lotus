package sealer

import (
	"context"
	"sync"

	sealtasks "github.com/filecoin-project/lotus/storage/sealer/sealtasks"
)

type WindowSelector func(sh *Scheduler, queueLen int, acceptableWindows [][]int, windows []SchedWindow) int

// AssignerCommon is a task assigner with customizable parts
type AssignerCommon struct {
	WindowSel WindowSelector
}

var _ Assigner = &AssignerCommon{}

func (a *AssignerCommon) TrySched(sh *Scheduler) {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.SchedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the SchedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through SchedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	//cachedWorkers := &schedWorkerCache{
	//	Workers: sh.Workers,
	//	cached:  map[storiface.WorkerID]*cachedSchedWorker{},
	//}
	//
	//windowsLen := len(sh.OpenWindows)
	//queueLen := sh.SchedQueue.Len()
	//
	//stats.Record(sh.mctx, metrics.SchedCycleOpenWindows.M(int64(windowsLen)))
	//stats.Record(sh.mctx, metrics.SchedCycleQueueSize.M(int64(queueLen)))
	//
	//log.Debugf("SCHED %d queued; %d open windows", queueLen, windowsLen)

	// schedule AddPiece Task
	a.trySchedAddPiece(sh)

	// schedule Groups
	a.trySchedGroups(sh)

	// schedule C2 Task
	a.trySchedC2(sh)
}

func (a *AssignerCommon) trySchedQueue(sh *Scheduler, queue []*WorkerRequest, dodo func(*Scheduler, *WorkerRequest) bool) (int, []*WorkerRequest) {
	queuneLen := len(queue)
	if queuneLen < 1 {
		return 0, nil
	}

	hasDoneSched := 0
	for i := 0; i < queuneLen; i++ {
		schReq := queue[i]
		if dodo(sh, schReq) {
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

func (a *AssignerCommon) trySchedAddPiece(sh *Scheduler) {
	queuneLen := len(sh.addPieceQueue)
	if queuneLen < 1 {
		return
	}

	log.Debugf("trySchedAddPiece begin, addPieceQueue len:%d", queuneLen)

	hasDoneSched, remainQueue := a.trySchedQueue(sh, sh.addPieceQueue, a.schedOneAddPiece)
	if hasDoneSched > 0 {
		sh.addPieceQueue = remainQueue
	}

	log.Debugf("trySchedAddPiece completed, sched done:%d", hasDoneSched)
}

func (a *AssignerCommon) schedOneAddPiece(sh *Scheduler, schReq *WorkerRequest) bool {
	taskType := schReq.TaskType
	selector, ok := schReq.Sel.(*addPieceSelector)
	var openWindowsGroup *schedWindowRequestsGroup
	var openWindowsTT []*SchedWindowRequest

	if ok {
		best := selector.findBestStorages()
		if len(best) < 1 {
			log.Debugf("schedOneAddPiece sector %d, taskType:%s, no available storage group found",
				schReq.Sector.ID.Number,
				taskType)
			return false
		}

		for _, store := range best {
			groupID := store.GroupID

			if groupID != "" {
				openWindowsGroup = sh.getOpenWindowsGroup(groupID)
				openWindowsTT, _ = openWindowsGroup.openWindows[schReq.TaskType]
			} else {
				continue
			}

			done, remainWindows := a.trySchedReq(sh, schReq, groupID, openWindowsTT)
			if done {
				openWindowsGroup.openWindows[schReq.TaskType] = remainWindows
				return true
			}
		}

		return false
	}

	groupID := schReq.Sel.GroupID()

	if groupID != "" {
		openWindowsGroup = sh.getOpenWindowsGroup(groupID)
		openWindowsTT, _ = openWindowsGroup.openWindows[schReq.TaskType]
	} else {
		log.Errorf("schedOneAddPiece sector %d, taskType:%s, no group configured for selector",
			schReq.Sector.ID.Number,
			taskType)
		return false
	}

	done, remainWindows := a.trySchedReq(sh, schReq, groupID, openWindowsTT)
	if done {
		openWindowsGroup.openWindows[schReq.TaskType] = remainWindows
		return true
	}

	return false
}

func (a *AssignerCommon) trySchedC2(sh *Scheduler) {
	queuneLen := len(sh.c2Queue)
	if queuneLen < 1 {
		return
	}

	log.Debugf("trySchedC2 begin, c2Queue len:%d", queuneLen)

	hasDoneSched, remainQueue := a.trySchedQueue(sh, sh.c2Queue, a.schedOneC2)
	if hasDoneSched > 0 {
		sh.c2Queue = remainQueue
	}

	log.Debugf("trySchedC2 completed, sched done:%d", hasDoneSched)
}

func (a *AssignerCommon) schedOneC2(sh *Scheduler, schReq *WorkerRequest) bool {
	var openWindowsTT []*SchedWindowRequest
	openWindowsTT = sh.openWindowsC2

	done, remainWindows := a.trySchedReq(sh, schReq, "", openWindowsTT)
	if done {
		sh.openWindowsC2 = remainWindows
	}

	return done
}

func (a *AssignerCommon) trySchedGroups(sh *Scheduler) {
	wg := &sync.WaitGroup{}
	wg.Add(len(sh.openWindowGroups))

	for groupID, openwindowGroup := range sh.openWindowGroups {
		gid := groupID
		openg := openwindowGroup

		go func() {
			tasks := openg.tasks
			update := make(map[sealtasks.TaskType][]*WorkerRequest)

			for tt, tarray := range tasks {
				log.Debugf("trySchedGroupTask begin, group %s, tasktype %s, queue len:%d",
					gid, tt, len(tarray))

				hasDoneSched, remainArray := a.trySchedGroupTask(sh, tt, gid,
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

func (a *AssignerCommon) trySchedGroupTask(sh *Scheduler, tasktype sealtasks.TaskType,
	groupID string,
	tarray []*WorkerRequest,
	g *schedWindowRequestsGroup) (int, []*WorkerRequest) {

	remainWindows, _ := g.openWindows[tasktype]
	if len(remainWindows) < 1 {
		return 0, tarray
	}

	// find open windows to handle worker reqeust
	handled := 0
	hasDone := false
	for i := 0; i < len(tarray); i++ {
		req := tarray[i]
		hasDone, remainWindows = a.trySchedReq(sh, req, groupID, remainWindows)
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

func (a *AssignerCommon) trySchedReq(sh *Scheduler, schReq *WorkerRequest, groupID string,
	openWindowsTT []*SchedWindowRequest) (result bool, windows []*SchedWindowRequest) {

	taskType := schReq.TaskType
	result = false
	windows = openWindowsTT

	if len(openWindowsTT) < 1 {
		log.Debugf("SCHED sector %d, taskType:%s, no available open window, group:%s",
			schReq.Sector.ID.Number,
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
					schReq.Sector.ID.Number,
					groupID)
				return
			}

			p1bucket = bucket
			log.Debugf("task acquire P1 ticket, sector:%d group:%s, remain:%d",
				schReq.Sector.ID.Number,
				groupID, bucket.atomicTikets)
		}
	}

	if taskType == sealtasks.TTFinalize && sh.finTicketInterval > 0 {
		if !sh.finTickets.use() {
			log.Debugf("task acquire Finalize ticket, sector:%d group:%s, no ticket remain",
				schReq.Sector.ID.Number,
				groupID)

			return
		}

		f1bucket = sh.finTickets
		log.Debugf("task acquire Finalize ticket, sector:%d group:%s, remain:%d",
			schReq.Sector.ID.Number,
			groupID, f1bucket.atomicTikets)
	}

	for wndIdx, availableWindow := range openWindowsTT {
		worker, ok := sh.Workers[availableWindow.Worker]
		if !ok {
			log.Errorf("worker referenced by windowRequest not found (worker: %s)",
				availableWindow.Worker)
			// TODO: How to move forward here?
			continue
		}

		if !worker.Enabled {
			log.Debugw("skipping disabled worker", "worker", availableWindow.Worker)
			continue
		}

		if _, paused := worker.paused[taskType]; paused {
			log.Debugw("skipping paused worker", "worker", availableWindow.Worker)
			continue
		}

		if taskType == sealtasks.TTCommit2 && availableWindow.groupID != "" {
			// check if group has P2, then P2 first
			g, kk := sh.openWindowGroups[availableWindow.groupID]
			if kk {
				p2Tasks, hasP2 := g.tasks[sealtasks.TTPreCommit2]
				if hasP2 && len(p2Tasks) > 0 {
					log.Debugf("C2 skipping worker that has P2 task, group:%s", availableWindow.groupID)
					continue
				}
			}
		}

		rpcCtx, cancel := context.WithTimeout(schReq.Ctx, SelectorTimeout)
		ok, _, err := schReq.Sel.Ok(rpcCtx, taskType, schReq.Sector.ProofType, worker)
		cancel()
		if err != nil {
			log.Errorf("trySched(1) sector:%d, group:%s, task-type:%s, req.sel.Ok error: %+v",
				schReq.Sector.ID.Number,
				availableWindow.groupID, taskType, err)
			continue
		}

		if !ok {
			// selector not allow
			continue
		}

		log.Debugf("SCHED assign sector %d to window %d, group:%s, task-type:%s",
			schReq.Sector.ID.Number,
			wndIdx, availableWindow.groupID, schReq.TaskType)

		window := SchedWindow{
			Todo:    schReq,
			groupID: availableWindow.groupID,
		}

		select {
		case availableWindow.Done <- &window:
		default:
			log.Errorf("expected sh.openWindows[wnd].done to be buffered, sector %d to window %d, group:%s, task-type:%s",
				schReq.Sector.ID.Number,
				wndIdx, availableWindow.groupID, schReq.TaskType)
			// found next available window
			continue
		}

		// done, remove that open window
		l := len(openWindowsTT)
		openWindowsTT[l-1], openWindowsTT[wndIdx] = openWindowsTT[wndIdx], openWindowsTT[l-1]
		// update windows
		result = true
		windows = openWindowsTT[0:(l - 1)]
		return
	}

	return
}
