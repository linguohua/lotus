package sectorstorage

// import (
// 	"sync"

// 	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
// 	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
// )

// func (a *activeResources) withResources(id WorkerID, wr storiface.WorkerResources,
// 	taskType sealtasks.TaskType, locker sync.Locker, cb func() error) error {
// 	for !a.canHandleRequest(wr, taskType, id, "withResources") {
// 		if a.cond == nil {
// 			a.cond = sync.NewCond(locker)
// 		}
// 		a.cond.Wait()
// 	}

// 	a.add(wr, taskType)

// 	err := cb()

// 	a.free(wr, taskType)

// 	if a.cond != nil {
// 		a.cond.Broadcast()
// 	}

// 	return err
// }

// func (a *activeResources) add(wr storiface.WorkerResources, taskType sealtasks.TaskType) {
// 	switch taskType {
// 	case sealtasks.TTAddPiece:
// 		a.AP = a.AP + 1
// 		if a.AP > wr.AP {
// 			log.Errorf("activeResources.add error, a.AP %d > wr.AP %d", a.AP, wr.AP)
// 		}
// 	case sealtasks.TTCommit1:
// 		a.C1 = a.C1 + 1
// 		if a.C1 > wr.C1 {
// 			log.Errorf("activeResources.add error, a.C1 %d > wr.C1 %d", a.C1, wr.C1)
// 		}
// 	case sealtasks.TTCommit2:
// 		a.C2 = a.C2 + 1
// 		if a.C2 > wr.C2 {
// 			log.Errorf("activeResources.add error, a.C2 %d > wr.C2 %d", a.C2, wr.C2)
// 		}
// 	case sealtasks.TTPreCommit1:
// 		a.P1 = a.P1 + 1
// 		if a.P1 > wr.P1 {
// 			log.Errorf("activeResources.add error, a.P1 %d > wr.P1 %d", a.P1, wr.P1)
// 		}
// 	case sealtasks.TTPreCommit2:
// 		a.P2 = a.P2 + 1
// 		if a.P2 > wr.P2 {
// 			log.Errorf("activeResources.add error, a.P2 %d > wr.P2 %d", a.P2, wr.P2)
// 		}
// 	case sealtasks.TTFinalize:
// 		a.FIN = a.FIN + 1
// 		if a.FIN > wr.FIN {
// 			log.Errorf("activeResources.add error, a.FIN %d > wr.FIN %d", a.FIN, wr.FIN)
// 		}
// 	}
// }

// func (a *activeResources) free(wr storiface.WorkerResources, taskType sealtasks.TaskType) {
// 	switch taskType {
// 	case sealtasks.TTAddPiece:
// 		a.AP = a.AP - 1
// 		if a.AP > wr.AP {
// 			log.Errorf("activeResources.free error, a.AP %d > wr.AP %d", a.AP, wr.AP)
// 		}
// 	case sealtasks.TTCommit1:
// 		a.C1 = a.C1 - 1
// 		if a.C1 > wr.C1 {
// 			log.Errorf("activeResources.free error, a.C1 %d > wr.C1 %d", a.C1, wr.C1)
// 		}
// 	case sealtasks.TTCommit2:
// 		a.C2 = a.C2 - 1
// 		if a.C2 > wr.C2 {
// 			log.Errorf("activeResources.free error, a.C2 %d > wr.C2 %d", a.C2, wr.C2)
// 		}
// 	case sealtasks.TTPreCommit1:
// 		a.P1 = a.P1 - 1
// 		if a.P1 > wr.P1 {
// 			log.Errorf("activeResources.free error, a.P1 %d > wr.P1 %d", a.P1, wr.P1)
// 		}
// 	case sealtasks.TTPreCommit2:
// 		a.P2 = a.P2 - 1
// 		if a.P2 > wr.P2 {
// 			log.Errorf("activeResources.free error, a.P2 %d > wr.P2 %d", a.P2, wr.P2)
// 		}
// 	case sealtasks.TTFinalize:
// 		a.FIN = a.FIN - 1
// 		if a.FIN > wr.FIN {
// 			log.Errorf("activeResources.free error, a.FIN %d > wr.FIN %d", a.FIN, wr.FIN)
// 		}
// 	}
// }

// func (a *activeResources) canHandleRequest(wr storiface.WorkerResources, taskType sealtasks.TaskType, wid WorkerID, caller string) bool {
// 	switch taskType {
// 	case sealtasks.TTAddPiece:
// 		if a.AP < wr.AP {
// 			return true
// 		}
// 	case sealtasks.TTCommit1:
// 		if a.C1 < wr.C1 {
// 			return true
// 		}
// 	case sealtasks.TTCommit2:
// 		if a.C2 < wr.C2 {
// 			return true
// 		}
// 	case sealtasks.TTPreCommit1:
// 		if a.P1 < wr.P1 {
// 			return true
// 		}
// 	case sealtasks.TTPreCommit2:
// 		if a.P2 < wr.P2 {
// 			return true
// 		}
// 	case sealtasks.TTFinalize:
// 		if a.FIN < wr.FIN {
// 			return true
// 		}
// 	}

// 	log.Debugf("activeResources.canHandleRequest no %s resource valid for worker:%s, caller:%s", taskType, wid, caller)
// 	return false
// }

// func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
// 	var max float64

// 	// cpu := float64(a.cpuUse) / float64(wr.CPUs)
// 	// max = cpu

// 	// memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
// 	// if memMin > max {
// 	// 	max = memMin
// 	// }

// 	// memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
// 	// if memMax > max {
// 	// 	max = memMax
// 	// }

// 	return max
// }


func (wh *workerHandle) utilization() float64 {
	// wh.lk.Lock()
	// u := wh.active.utilization(wh.info.Resources)
	// // u += wh.preparing.utilization(wh.info.Resources)
	// wh.lk.Unlock()
	// wh.wndLk.Lock()
	// // for _, window := range wh.activeWindows {
	// // 	u += window.allocated.utilization(wh.info.Resources)
	// // }
	// wh.wndLk.Unlock()

	// return u
	return 0
}
