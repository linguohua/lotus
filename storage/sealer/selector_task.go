package sealer

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type taskSelector struct {
	best        []storiface.StorageInfo //nolint: unused, structcheck
	queryWorker bool
}

func newTaskSelector(queryWorker bool) *taskSelector {
	return &taskSelector{
		queryWorker: queryWorker,
	}
}

func (s *taskSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd SchedWorker) (bool, bool, error) {
	supported := false
	tasks, err := whnd.TaskTypes(ctx)
	if err != nil {
		return false, false, err
	}

	_, supported = tasks[task]
	if !supported {
		return false, false, nil
	}

	if s.queryWorker {
		if false == whnd.RemoteWorkerHasResourceForNewTask(ctx, task) {
			return false, false, nil
		}
	}

	return true, false, nil
}

func (s *taskSelector) Cmp(ctx context.Context, _ sealtasks.TaskType, a, b SchedWorker) (bool, error) {
	//atasks, err := a.TaskTypes(ctx)
	//if err != nil {
	//	return false, xerrors.Errorf("getting supported worker task types: %w", err)
	//}

	//btasks, err := b.TaskTypes(ctx)
	//if err != nil {
	//	return false, xerrors.Errorf("getting supported worker task types: %w", err)
	//}
	//if len(atasks) != len(btasks) {
	//	return len(atasks) < len(btasks), nil // prefer workers which can do less
	//}

	return a.Utilization() < b.Utilization(), nil
}

func (s *taskSelector) GroupID() string {
	return ""
}

var _ WorkerSelector = &taskSelector{}
