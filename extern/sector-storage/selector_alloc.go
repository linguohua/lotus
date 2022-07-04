package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type allocSelector struct {
	index stores.SectorIndex
	alloc storiface.SectorFileType
	ptype storiface.PathType
}

func newAllocSelector(index stores.SectorIndex, alloc storiface.SectorFileType, ptype storiface.PathType) *allocSelector {
	return &allocSelector{
		index: index,
		alloc: alloc,
		ptype: ptype,
	}
}

func (s *allocSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
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

	// paths, err := whnd.workerRpc.Paths(ctx)
	// if err != nil {
	// 	return false, xerrors.Errorf("getting worker paths: %w", err)
	// }

	// have := map[storiface.ID]struct{}{}
	// for _, path := range paths {
	// 	have[path.ID] = struct{}{}
	// }

	ssize, err := spt.SectorSize()
	if err != nil {
		return false, xerrors.Errorf("getting sector size: %w", err)
	}

	best, err := s.index.StorageBestAlloc(ctx, s.alloc, ssize, s.ptype)
	if err != nil {
		return false, xerrors.Errorf("finding best alloc storage: %w", err)
	}

	workerGroupID := whnd.info.GroupID
	for _, info := range best {
		// if _, ok := have[info.ID]; ok {
		// 	return true, nil
		// }
		if info.GroupID == "" {
			// can bind to any worker
			//log.Infof("found match worker and free bind storage, worker group id:%s", workerGroupID)
			return true, nil
		} else {
			if info.GroupID == workerGroupID {
				//log.Infof("found match worker and storage, group id:%s", workerGroupID)
				return true, nil
			}
		}
	}

	return false, nil
}

func (s *allocSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

func (s *allocSelector) GroupID() string {
	return ""
}

var _ WorkerSelector = &allocSelector{}

type addPieceSelector struct {
	index  stores.SectorIndex
	alloc  storiface.SectorFileType
	ptype  storiface.PathType
	sector storage.SectorRef
}

func newAddPieceSelector(index stores.SectorIndex,
	sector storage.SectorRef,
	alloc storiface.SectorFileType, ptype storiface.PathType) *addPieceSelector {

	return &addPieceSelector{
		index:  index,
		alloc:  alloc,
		ptype:  ptype,
		sector: sector,
	}
}

func (s *addPieceSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	return true, nil
}

func (s *addPieceSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

func (s *addPieceSelector) GroupID() string {
	return ""
}

func (s *addPieceSelector) findBestStorages() []storiface.StorageInfo {
	spt := s.sector.ProofType
	ssize, err := spt.SectorSize()
	if err != nil {
		log.Errorf("addPieceSelector: sector:%v, getting sector size: %v", s.sector.ID, err)
		return nil
	}

	// try to find best group
	ctx := context.TODO()
	best, err := s.index.StorageBestAlloc(ctx, s.alloc, ssize, s.ptype)
	if err != nil {
		log.Errorf("addPieceSelector: sector:%v, finding best alloc storage: %v", s.sector.ID, err)
		return nil
	}

	return best
}

var _ WorkerSelector = &addPieceSelector{}
