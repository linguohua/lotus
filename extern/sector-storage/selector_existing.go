package sectorstorage

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	xerrors "golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type existingSelector struct {
	index   stores.SectorIndex
	sector  abi.SectorID
	alloc   storiface.SectorFileType
	groupID string
}

func findSectorGroup(ctx context.Context, index stores.SectorIndex, spt abi.RegisteredSealProof,
	sector abi.SectorID, alloc storiface.SectorFileType) (string, error) {
	ssize, err := spt.SectorSize()
	if err != nil {
		return "", xerrors.Errorf("findSectorGroup getting sector size: %w", err)
	}

	best, err := index.StorageFindSector(ctx, sector, alloc, ssize, false)
	if err != nil {
		return "", xerrors.Errorf("findSectorGroup: finding best storage error: %w", err)
	}

	if len(best) < 1 {
		// log.Errorf("existingSelector.ok StorageFindSector found none, sector:%s task type:%s", s.sector, task)
		return "", xerrors.Errorf("findSectorGroup no valid storage found for sector:%s")
	}

	for _, s := range best {
		if s.GroupID != "" {
			log.Debugf("findSectorGroup ok, sector:%s, group:%s", sector, s.GroupID)
			return s.GroupID, nil
		}
	}

	log.Errorf("findSectorGroup failed, sector:%s, non-group found", sector)
	return "", nil
}

func newExistingSelector(index stores.SectorIndex, sector abi.SectorID, alloc storiface.SectorFileType, groupID string) *existingSelector {
	if groupID == "" {
		log.Warnf("newExistingSelector, sector:%s, group should not be empty", sector)
	}

	return &existingSelector{
		index:   index,
		sector:  sector,
		alloc:   alloc,
		groupID: groupID,
	}
}

func (s *existingSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	//tasks, err := whnd.workerRpc.TaskTypes(ctx)
	//if err != nil {
	//	return false, xerrors.Errorf("getting supported worker task types: %w", err)
	//}
	supported := false
	tasks := whnd.acceptTaskTypes
	for _, t := range tasks {
		if t == task {
			supported = true
			break
		}
	}

	if !supported {
		return false, nil
	}

	//paths, err := whnd.workerRpc.Paths(ctx)
	//if err != nil {
	//	return false, xerrors.Errorf("getting worker paths: %w", err)
	//}

	// have := map[stores.ID]struct{}{}
	// for _, path := range paths {
	// 	have[path.ID] = struct{}{}
	// }

	// ssize, err := spt.SectorSize()
	// if err != nil {
	// 	return false, xerrors.Errorf("getting sector size: %w", err)
	// }

	// best, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, ssize, false)
	// if err != nil {
	// 	return false, xerrors.Errorf("finding best storage: %w", err)
	// }

	// if len(best) < 1 {
	// 	// log.Errorf("existingSelector.ok StorageFindSector found none, sector:%s task type:%s", s.sector, task)
	// 	return false, nil
	// }

	workerGroupID := whnd.info.GroupID
	if workerGroupID == s.groupID {
		return true, nil
	}

	// for _, info := range best {
	// 	// if _, ok := have[info.ID]; ok {
	// 	// 	return true, nil
	// 	// }
	// 	if info.GroupID == "" {
	// 		// can bind to any worker
	// 		//log.Infof("found match worker and free bind storage, worker group id:%s", workerGroupID)
	// 		return true, nil
	// 	} else {
	// 		if info.GroupID == workerGroupID {
	// 			return true, nil
	// 		}
	// 	}

	// 	//log.Infof("existingSelector.ok group id not match info:%s != worker:%d", info.GroupID, workerGroupID)
	// }

	//log.Infof("existingSelector.ok return false, task type:%s", task)
	return false, nil
}

func (s *existingSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

func (s *existingSelector) GroupID() string {
	return s.groupID
}

var _ WorkerSelector = &existingSelector{}
