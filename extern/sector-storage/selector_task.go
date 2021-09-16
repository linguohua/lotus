package sectorstorage

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

type taskSelector struct {
	best        []stores.StorageInfo //nolint: unused, structcheck
	queryWorker bool
}

func newTaskSelector(queryWorker bool) *taskSelector {
	return &taskSelector{
		queryWorker: queryWorker,
	}
}

func (s *taskSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
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

	if s.queryWorker {
		if false == whnd.workerRpc.HasResourceForNewTask(ctx, task) {
			return false, nil
		}
	}

	return true, nil
}

func (s *taskSelector) Cmp(ctx context.Context, _ sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	// atasks, err := a.workerRpc.TaskTypes(ctx)
	// if err != nil {
	// 	return false, xerrors.Errorf("getting supported worker task types: %w", err)
	// }
	// btasks, err := b.workerRpc.TaskTypes(ctx)
	// if err != nil {
	// 	return false, xerrors.Errorf("getting supported worker task types: %w", err)
	// }
	// if len(atasks) != len(btasks) {
	// 	return len(atasks) < len(btasks), nil // prefer workers which can do less
	// }

	return a.utilization() < b.utilization(), nil
}

func (s *taskSelector) GroupID() string {
	return ""
}

var _ WorkerSelector = &taskSelector{}
