package sealer

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/storage/paths"
	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type allocSelector struct {
	index paths.SectorIndex
	alloc storiface.SectorFileType
	ptype storiface.PathType
}

func newAllocSelector(index paths.SectorIndex, alloc storiface.SectorFileType, ptype storiface.PathType) *allocSelector {
	return &allocSelector{
		index: index,
		alloc: alloc,
		ptype: ptype,
	}
}

func (s *allocSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd SchedWorker) (bool, bool, error) {
	supported := false
	tasks, err := whnd.TaskTypes(ctx)
	if err != nil {
		return false, false, err
	}

	_, supported = tasks[task]
	if !supported {
		return false, false, nil
	}

	ssize, err := spt.SectorSize()
	if err != nil {
		return false, false, xerrors.Errorf("getting sector size: %w", err)
	}

	best, err := s.index.StorageBestAlloc(ctx, s.alloc, ssize, s.ptype)
	if err != nil {
		return false, false, xerrors.Errorf("finding best alloc storage: %w", err)
	}

	workerGroupID := whnd.GroupID()
	for _, info := range best {
		if info.GroupID == "" {
			// can bind to any worker
			//log.Infof("found match worker and free bind storage, worker group id:%s", workerGroupID)
			return true, true, nil
		} else {
			if info.GroupID == workerGroupID {
				//log.Infof("found match worker and storage, group id:%s", workerGroupID)
				return true, true, nil
			}
		}
	}

	return false, false, nil
}

func (s *allocSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b SchedWorker) (bool, error) {
	return a.Utilization() < b.Utilization(), nil
}

func (s *allocSelector) GroupID() string {
	return ""
}

var _ WorkerSelector = &allocSelector{}

type addPieceSelector struct {
	index  paths.SectorIndex
	alloc  storiface.SectorFileType
	ptype  storiface.PathType
	sector storiface.SectorRef
}

func newAddPieceSelector(index paths.SectorIndex,
	sector storiface.SectorRef,
	alloc storiface.SectorFileType, ptype storiface.PathType) *addPieceSelector {

	return &addPieceSelector{
		index:  index,
		alloc:  alloc,
		ptype:  ptype,
		sector: sector,
	}
}

func (s *addPieceSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd SchedWorker) (bool, bool, error) {
	return true, true, nil
}

func (s *addPieceSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b SchedWorker) (bool, error) {
	return a.Utilization() < b.Utilization(), nil
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
