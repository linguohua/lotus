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
	index       stores.SectorIndex
	sector      abi.SectorID
	alloc       storiface.SectorFileType
	groupID     string
	queryWorker bool
}

func findSectorGroup(ctx context.Context, index stores.SectorIndex, spt abi.RegisteredSealProof,
	sector abi.SectorID, alloc storiface.SectorFileType) (string, error) {
	ssize, err := spt.SectorSize()
	if err != nil {
		return "", xerrors.Errorf("findSectorGroup getting sector size: %v, sector:%v", ssize, sector)
	}

	best, err := index.StorageFindSector(ctx, sector, alloc, ssize, false)
	if err != nil {
		return "", xerrors.Errorf("findSectorGroup: finding best storage error: %v, sector:%v", err, sector)
	}

	if len(best) < 1 {
		// log.Errorf("existingSelector.ok StorageFindSector found none, sector:%s task type:%s", s.sector, task)
		return "", xerrors.Errorf("findSectorGroup no valid storage found for sector:%v", sector)
	}

	for _, s := range best {
		if s.GroupID != "" {
			log.Debugf("findSectorGroup ok, sector:%v, group:%s", sector, s.GroupID)
			return s.GroupID, nil
		}
	}

	log.Errorf("findSectorGroup failed, sector:%v, non-group found", sector)
	return "", nil
}

func newExistingSelector(queryWorker bool, index stores.SectorIndex, sector abi.SectorID, alloc storiface.SectorFileType, groupID string) *existingSelector {
	if groupID == "" {
		log.Warnf("newExistingSelector, sector:%s, group should not be empty", sector)
	}

	return &existingSelector{
		index:       index,
		sector:      sector,
		alloc:       alloc,
		groupID:     groupID,
		queryWorker: queryWorker,
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

	workerGroupID := whnd.info.GroupID
	if workerGroupID != s.groupID {
		return false, nil
	}

	if s.queryWorker {
		if task == sealtasks.TTPreCommit1 || task == sealtasks.TTPreCommit2 ||
			task == sealtasks.TTCommit2 {

			if false == whnd.workerRpc.HasResourceForNewTask(ctx, task) {
				return false, nil
			}
		}
	}

	return true, nil
}

func (s *existingSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

func (s *existingSelector) GroupID() string {
	return s.groupID
}

var _ WorkerSelector = &existingSelector{}
