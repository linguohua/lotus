package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

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

	// have := map[stores.ID]struct{}{}
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
	index   stores.SectorIndex
	alloc   storiface.SectorFileType
	ptype   storiface.PathType
	groupID string
}

func newAddPieceSelector(ctx context.Context, index stores.SectorIndex,
	spt abi.RegisteredSealProof, alloc storiface.SectorFileType, ptype storiface.PathType) (*addPieceSelector, error) {

	ssize, err := spt.SectorSize()
	if err != nil {
		return nil, xerrors.Errorf("getting sector size: %w", err)
	}

	// try to find best group
	best, err := index.StorageBestAlloc(ctx, alloc, ssize, ptype)
	if err != nil {
		return nil, xerrors.Errorf("finding best alloc storage: %v", err)
	}

	groupID := ""
	for _, info := range best {
		// if _, ok := have[info.ID]; ok {
		// 	return true, nil
		// }
		if info.GroupID != "" {
			// can bind to any worker
			//log.Infof("found match worker and free bind storage, worker group id:%s", workerGroupID)
			groupID = info.GroupID
			break
		}
	}

	if groupID == "" {
		return nil, xerrors.Errorf("cant found best alloc storage")
	}

	return &addPieceSelector{
		index:   index,
		alloc:   alloc,
		ptype:   ptype,
		groupID: groupID,
	}, nil
}

func (s *addPieceSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
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

	// have := map[stores.ID]struct{}{}
	// for _, path := range paths {
	// 	have[path.ID] = struct{}{}
	// }
	workerGroupID := whnd.info.GroupID
	if workerGroupID == s.groupID {
		return true, nil
	}

	return false, nil
}

func (s *addPieceSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

func (s *addPieceSelector) GroupID() string {
	return s.groupID
}

var _ WorkerSelector = &addPieceSelector{}
