package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"

	"golang.org/x/sync/semaphore"
)

var pathTypes = []storiface.SectorFileType{storiface.FTUnsealed, storiface.FTSealed, storiface.FTCache, storiface.FTUpdate, storiface.FTUpdateCache}

type WorkerConfig struct {
	TaskTypes []sealtasks.TaskType
	NoSwap    bool

	// IgnoreResourceFiltering enables task distribution to happen on this
	// worker regardless of its currently available resources. Used in testing
	// with the local worker.
	IgnoreResourceFiltering bool
}

// used do provide custom proofs impl (mostly used in testing)
type ExecutorFunc func() (ffiwrapper.Storage, error)
type EnvFunc func(string) (string, bool)

type LocalWorkerExtParams struct {
	PieceTemplateSize abi.SectorSize
	PieceTemplateDir  string
	MerkleTreecache   string

	GroupID string
	Role    string

	C2Count int
}

type LocalWorker struct {
	pieceTemplateSize abi.SectorSize
	pieceTemplateDir  string
	merkleTreecache   string

	groupID string

	storage    stores.Store
	localStore *stores.Local
	sindex     stores.SectorIndex
	ret        storiface.WorkerReturn
	executor   ExecutorFunc
	noSwap     bool
	envLookup  EnvFunc

	// see equivalent field on WorkerConfig.
	ignoreResources bool

	ct           *workerCallTracker
	runningTasks map[sealtasks.TaskType]int
	acceptTasks  map[sealtasks.TaskType]struct{}
	running      sync.WaitGroup
	taskLk       sync.Mutex

	session     uuid.UUID
	testDisable int64
	closing     chan struct{}

	chanCacheClear chan struct {
		string
		uint64
	}

	// p2, c2 exclusive
	p2c2Semaphore *semaphore.Weighted
	// p1 exclusive
	p1Semaphore *semaphore.Weighted

	role    string
	c2Count int
}

func loadP1CountFromEnv() int {
	if os.Getenv("FIL_PROOFS_STEP_STYLE") != "true" {
		return 1
	}

	stepsEnv := os.Getenv("FIL_PROOFS_STEPS")
	if len(stepsEnv) < 1 {
		log.Fatal("FIL_PROOFS_STEPS should be configured when FIL_PROOFS_STEP_STYLE=true")
	}

	steps := strings.Split(stepsEnv, ",")
	return len(steps)
}

func newLocalWorker(executor ExecutorFunc, wcfg WorkerConfig,
	store stores.Store, local *stores.Local, sindex stores.SectorIndex,
	ret storiface.WorkerReturn, cst *statestore.StateStore, ext *LocalWorkerExtParams) *LocalWorker {
	log.Infof("newLocalWorker executor:%v, wcfg:%+v, ext:%+v", executor, wcfg, ext)

	acceptTasks := map[sealtasks.TaskType]struct{}{}
	for _, taskType := range wcfg.TaskTypes {
		acceptTasks[taskType] = struct{}{}
	}

	w := &LocalWorker{
		storage:    store,
		localStore: local,
		sindex:     sindex,
		ret:        ret,

		ct: &workerCallTracker{
			st: cst,
		},

		// envLookup:       envLookup,
		ignoreResources: wcfg.IgnoreResourceFiltering,

		acceptTasks:  acceptTasks,
		runningTasks: make(map[sealtasks.TaskType]int),
		executor:     executor,
		noSwap:       wcfg.NoSwap,

		session: uuid.New(),
		closing: make(chan struct{}),

		chanCacheClear: make(chan struct {
			string
			uint64
		}, 16),

		c2Count: 1,
	}

	if ext != nil {
		w.groupID = ext.GroupID
		w.pieceTemplateDir = ext.PieceTemplateDir
		w.pieceTemplateSize = ext.PieceTemplateSize
		w.merkleTreecache = ext.MerkleTreecache
		w.c2Count = ext.C2Count
	}

	// reset c2 count
	parallelConfig[sealtasks.TTCommit2] = uint32(w.c2Count)

	if w.executor == nil {
		w.executor = w.ffiExec
	}

	w.p2c2Semaphore = semaphore.NewWeighted(int64(w.c2Count))

	unfinished, err := w.ct.unfinished()
	if err != nil {
		log.Errorf("reading unfinished tasks: %+v", err)
		return w
	}

	go func() {
		for _, call := range unfinished {
			err := storiface.Err(storiface.ErrTempWorkerRestart, xerrors.New("worker restarted"))

			// TODO: Handle restarting PC1 once support is merged

			if doReturn(context.TODO(), call.RetType, call.ID, ret, nil, err) {
				if err := w.ct.onReturned(call.ID); err != nil {
					log.Errorf("marking call as returned failed: %s: %+v", call.RetType, err)
				}
			}
		}
	}()

	go func() {
		for {
			cached, ok := <-w.chanCacheClear
			if !ok {
				log.Info("LocalWorker.chanCacheClear closed")
				break
			}

			log.Info("LocalWorker.chanCacheClear, do:", cached.string)

			// lingh: we clear cache after C1 completed
			err = ffi.ClearCache(uint64(cached.uint64), cached.string)
			if err != nil {
				log.Warnf("StandaloneSealCommit: ffi.ClearCache failed with error:%v, cache maybe removed previous", err)
			}

			log.Infof("LocalWorker.chanCacheClear, do:%s completed", cached.string)
		}
	}()

	if ext != nil {
		w.role = ext.Role
	}

	if w.role == "P1" {
		log.Info("LocalWorker.New role is P1, try allocate hugepages for 64GB sectors")
		sn := abi.SectorNumber(0)
		mid := abi.ActorID(0)
		ti := abi.SealRandomness([]byte{0})
		pi := []abi.PieceInfo{}

		stype := abi.RegisteredSealProof_StackedDrg64GiBV1
		if os.Getenv("SECTOR_TYPE") == "32GB" {
			stype = abi.RegisteredSealProof_StackedDrg32GiBV1
		}
		_, err = ffi.SealPreCommitPhase1(stype,
			"hpalloc", "hpalloc", "hpalloc", sn, mid, ti, pi)
		if err != nil && err.Error() != "ok" {
			log.Fatalf("LocalWorker.New role is P1, allocate hugepages failed:%v", err)
		} else {
			log.Infof("LocalWorker.New role is P1, try allocate completed: ", err)
		}

		// parse p1 count
		var p1Count = loadP1CountFromEnv()
		log.Infof("worker use P1 count:%d", p1Count)
		// reset P1 count
		parallelConfig[sealtasks.TTPreCommit1] = uint32(p1Count)
		// new P1 semaphore
		w.p1Semaphore = semaphore.NewWeighted(int64(p1Count))
	}

	// if w.role == "C2" || w.role == "P2C2" {
	// 	sn := abi.SectorNumber(0)
	// 	mid := abi.ActorID(0)
	// 	ti := abi.SealRandomness([]byte{0})
	// 	pi := []abi.PieceInfo{}

	// 	stype := abi.RegisteredSealProof_StackedDrg64GiBV1
	// 	if os.Getenv("SECTOR_TYPE") == "32GB" {
	// 		stype = abi.RegisteredSealProof_StackedDrg32GiBV1
	// 	}
	// 	_, err = ffi.SealPreCommitPhase1(stype,
	// 		"c2warm", "c2warm", "c2warm", sn, mid, ti, pi)

	// 	if err != nil && err.Error() != "ok" {
	// 		log.Fatalf("LocalWorker.New role is P2C2, warmup failed:%v", err)
	// 	} else {
	// 		log.Infof("LocalWorker.New role is P2C2, warmup completed: ", err)
	// 	}
	// }

	return w
}

func NewLocalWorker(wcfg WorkerConfig, store stores.Store, local *stores.Local, sindex stores.SectorIndex,
	ret storiface.WorkerReturn, cst *statestore.StateStore, ext *LocalWorkerExtParams) *LocalWorker {
	return newLocalWorker(nil, wcfg, store, local, sindex, ret, cst, ext)
}

type localWorkerPathProvider struct {
	w  *LocalWorker
	op storiface.AcquireMode
}

func (l *localWorkerPathProvider) AcquireSector(ctx context.Context, sector storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, sealing storiface.PathType) (storiface.SectorPaths, func(), error) {
	paths, storageIDs, err := l.w.storage.AcquireSector(ctx, sector, existing, allocate, sealing, l.op)
	if err != nil {
		return storiface.SectorPaths{}, nil, err
	}

	// releaseStorage, err := l.w.localStore.Reserve(ctx, sector, allocate, storageIDs, storiface.FSOverheadSeal)
	// if err != nil {
	// 	return storiface.SectorPaths{}, nil, xerrors.Errorf("reserving storage space: %w", err)
	// }

	log.Debugf("acquired sector %d (e:%d; a:%d): %v", sector, existing, allocate, paths)

	return paths, func() {
		// releaseStorage()

		for _, fileType := range pathTypes {
			if fileType&allocate == 0 {
				continue
			}

			sid := storiface.PathByType(storageIDs, fileType)
			if err := l.w.sindex.StorageDeclareSector(ctx, stores.ID(sid), sector.ID, fileType, l.op == storiface.AcquireMove); err != nil {
				log.Errorf("declare sector error: %+v", err)
			}
		}
	}, nil
}

func (l *localWorkerPathProvider) DiscoverSectorStore(ctx context.Context, id abi.SectorID) error {
	return fmt.Errorf("DiscoverSectorStore not implemented for localWorkerPathProvider")
}

func (l *LocalWorker) ffiExec() (ffiwrapper.Storage, error) {
	ccfunc := func(cache string, size uint64) {
		l.clearLocalCache(cache, size)
	}

	return ffiwrapper.New(&localWorkerPathProvider{w: l}, l.merkleTreecache, ccfunc)
}

func (l *LocalWorker) clearLocalCache(cache string, size uint64) {
	if l.chanCacheClear != nil {
		log.Info("LocalWorker.clearLocalCache:", cache)
		go func() {
			defer func() {
				if recover() == nil {
					return
				}
			}()

			l.chanCacheClear <- struct {
				string
				uint64
			}{cache, size}
		}()
	} else {
		log.Infof("LocalWorker.clearLocalCache:%s, but no chanCacheClear avalible", cache)
	}
}

type ReturnType string

const (
	AddPiece              ReturnType = "AddPiece"
	SealPreCommit1        ReturnType = "SealPreCommit1"
	SealPreCommit2        ReturnType = "SealPreCommit2"
	SealCommit1           ReturnType = "SealCommit1"
	SealCommit2           ReturnType = "SealCommit2"
	FinalizeSector        ReturnType = "FinalizeSector"
	FinalizeReplicaUpdate ReturnType = "FinalizeReplicaUpdate"
	ReplicaUpdate         ReturnType = "ReplicaUpdate"
	ProveReplicaUpdate1   ReturnType = "ProveReplicaUpdate1"
	ProveReplicaUpdate2   ReturnType = "ProveReplicaUpdate2"
	GenerateSectorKey     ReturnType = "GenerateSectorKey"
	ReleaseUnsealed       ReturnType = "ReleaseUnsealed"
	MoveStorage           ReturnType = "MoveStorage"
	UnsealPiece           ReturnType = "UnsealPiece"
	Fetch                 ReturnType = "Fetch"
)

// in: func(WorkerReturn, context.Context, CallID, err string)
// in: func(WorkerReturn, context.Context, CallID, ret T, err string)
func rfunc(in interface{}) func(context.Context, storiface.CallID, storiface.WorkerReturn, interface{}, *storiface.CallError) error {
	rf := reflect.ValueOf(in)
	ft := rf.Type()
	withRet := ft.NumIn() == 5

	return func(ctx context.Context, ci storiface.CallID, wr storiface.WorkerReturn, i interface{}, err *storiface.CallError) error {
		rctx := reflect.ValueOf(ctx)
		rwr := reflect.ValueOf(wr)
		rerr := reflect.ValueOf(err)
		rci := reflect.ValueOf(ci)

		var ro []reflect.Value

		if withRet {
			ret := reflect.ValueOf(i)
			if i == nil {
				ret = reflect.Zero(rf.Type().In(3))
			}

			ro = rf.Call([]reflect.Value{rwr, rctx, rci, ret, rerr})
		} else {
			ro = rf.Call([]reflect.Value{rwr, rctx, rci, rerr})
		}

		if !ro[0].IsNil() {
			return ro[0].Interface().(error)
		}

		return nil
	}
}

var returnFunc = map[ReturnType]func(context.Context, storiface.CallID, storiface.WorkerReturn, interface{}, *storiface.CallError) error{
	AddPiece:              rfunc(storiface.WorkerReturn.ReturnAddPiece),
	SealPreCommit1:        rfunc(storiface.WorkerReturn.ReturnSealPreCommit1),
	SealPreCommit2:        rfunc(storiface.WorkerReturn.ReturnSealPreCommit2),
	SealCommit1:           rfunc(storiface.WorkerReturn.ReturnSealCommit1),
	SealCommit2:           rfunc(storiface.WorkerReturn.ReturnSealCommit2),
	FinalizeSector:        rfunc(storiface.WorkerReturn.ReturnFinalizeSector),
	ReleaseUnsealed:       rfunc(storiface.WorkerReturn.ReturnReleaseUnsealed),
	ReplicaUpdate:         rfunc(storiface.WorkerReturn.ReturnReplicaUpdate),
	ProveReplicaUpdate1:   rfunc(storiface.WorkerReturn.ReturnProveReplicaUpdate1),
	ProveReplicaUpdate2:   rfunc(storiface.WorkerReturn.ReturnProveReplicaUpdate2),
	GenerateSectorKey:     rfunc(storiface.WorkerReturn.ReturnGenerateSectorKeyFromData),
	FinalizeReplicaUpdate: rfunc(storiface.WorkerReturn.ReturnFinalizeReplicaUpdate),
	MoveStorage:           rfunc(storiface.WorkerReturn.ReturnMoveStorage),
	UnsealPiece:           rfunc(storiface.WorkerReturn.ReturnUnsealPiece),
	Fetch:                 rfunc(storiface.WorkerReturn.ReturnFetch),
}

func (l *LocalWorker) asyncCall(ctx context.Context, sector storage.SectorRef, rt ReturnType, work func(ctx context.Context, ci storiface.CallID) (interface{}, error)) (storiface.CallID, error) {
	ci := storiface.CallID{
		Sector: sector.ID,
		ID:     uuid.New(),
	}

	if err := l.ct.onStart(ci, rt); err != nil {
		log.Errorf("tracking call (start): %+v", err)
	}

	l.running.Add(1)

	go func() {
		defer l.running.Done()

		ctx := &wctx{
			vals:    ctx,
			closing: l.closing,
		}

		res, err := work(ctx, ci)
		if err != nil {
			rb, err := json.Marshal(res)
			if err != nil {
				log.Errorf("tracking call (marshaling results): %+v", err)
			} else {
				if err := l.ct.onDone(ci, rb); err != nil {
					log.Errorf("tracking call (done): %+v", err)
				}
			}
		}

		if doReturn(ctx, rt, ci, l.ret, res, toCallError(err)) {
			if err := l.ct.onReturned(ci); err != nil {
				log.Errorf("tracking call (done): %+v", err)
			}
		}
	}()
	return ci, nil
}

func toCallError(err error) *storiface.CallError {
	var serr *storiface.CallError
	if err != nil && !xerrors.As(err, &serr) {
		serr = storiface.Err(storiface.ErrUnknown, err)
	}

	return serr
}

// doReturn tries to send the result to manager, returns true if successful
func doReturn(ctx context.Context, rt ReturnType, ci storiface.CallID, ret storiface.WorkerReturn, res interface{}, rerr *storiface.CallError) bool {
	for {
		err := returnFunc[rt](ctx, ci, ret, res, rerr)
		if err == nil {
			break
		}

		log.Errorf("return error, will retry in 5s: %s: %+v", rt, err)
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			log.Errorf("failed to return results: %s", ctx.Err())

			// fine to just return, worker is most likely shutting down, and
			// we didn't mark the result as returned yet, so we'll try to
			// re-submit it on restart
			return false
		}
	}

	return true
}

func (l *LocalWorker) NewSector(ctx context.Context, sector storage.SectorRef) error {
	sb, err := l.executor()
	if err != nil {
		return err
	}

	return sb.NewSector(ctx, sector)
}

func (l *LocalWorker) AddPiece(ctx context.Context, sector storage.SectorRef, epcs []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	size, _ := sector.ProofType.SectorSize()
	hasTemplate := l.hasPieceTemplate()

	log.Debugf("AddPiece size: %d, hasTemplate: %v, pieceTemplateSize: %d", size, hasTemplate, l.pieceTemplateSize)
	if hasTemplate && size <= l.pieceTemplateSize {
		return l.asyncCall(ctx, sector, AddPiece, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
			return l.loadPieceTemplate(ctx, sector)
		})
	}

	return l.asyncCall(ctx, sector, AddPiece, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.AddPiece(ctx, sector, epcs, sz, r)
	})
}

func (l *LocalWorker) hasPieceTemplate() bool {
	if l.pieceTemplateDir == "" {
		return false
	}

	pieceFilePath := path.Join(l.pieceTemplateDir, "staged-file")
	pieceinfos := path.Join(l.pieceTemplateDir, "piece-info.json")

	_, err := os.Stat(pieceFilePath)
	if os.IsNotExist(err) {
		return false
	}

	_, err = os.Stat(pieceinfos)
	if os.IsNotExist(err) {
		return false
	}

	return true
}

func (l *LocalWorker) loadPieceTemplate(ctx context.Context, sector storage.SectorRef) (abi.PieceInfo, error) {
	log.Debugf("loadPieceTemplate call, sector:%v", sector)

	stagedPath, done, err := (&localWorkerPathProvider{w: l}).AcquireSector(ctx, sector, 0, storiface.FTUnsealed, storiface.PathSealing)
	if err != nil {
		log.Errorf("loadPieceTemplate AcquireSector failed:%v", err)
		return abi.PieceInfo{}, err
	}

	defer func() {
		if done != nil {
			done()
		}
	}()

	pieceFilePath := path.Join(l.pieceTemplateDir, "staged-file")
	pieceinfos := path.Join(l.pieceTemplateDir, "piece-info.json")

	// soft link file to staged path
	err = os.Symlink(pieceFilePath, stagedPath.Unsealed)
	if err != nil {
		log.Errorf("loadPieceTemplate Symlink failed:%v", err)
		return abi.PieceInfo{}, xerrors.Errorf("loadPieceTemplate %w", err)
	}

	// read pieceCID from json file
	bb, err := ioutil.ReadFile(pieceinfos)
	if err != nil {
		log.Errorf("loadPieceTemplate ReadAll failed:%v", err)
		return abi.PieceInfo{}, xerrors.Errorf("loadPieceTemplate: %w", err)
	}

	pi := abi.PieceInfo{}
	err = json.Unmarshal(bb, &pi)
	if err != nil {
		log.Errorf("loadPieceTemplate Unmarshal failed:%v", err)
		return abi.PieceInfo{}, xerrors.Errorf("loadPieceTemplate: %w", err)
	}

	log.Debugf("loadPieceTemplate completed, sector:%v", sector)
	return pi, nil
}

func (l *LocalWorker) Fetch(ctx context.Context, sector storage.SectorRef, fileType storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, Fetch, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		_, done, err := (&localWorkerPathProvider{w: l, op: am}).AcquireSector(ctx, sector, fileType, storiface.FTNone, ptype)
		if err == nil {
			done()
		}

		return nil, err
	})
}

func (l *LocalWorker) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, SealPreCommit1, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {

		{
			// cleanup previous failed attempts if they exist
			if err := l.storage.Remove(ctx, sector.ID, storiface.FTSealed, true, nil); err != nil {
				return nil, xerrors.Errorf("cleaning up sealed data: %w", err)
			}

			if err := l.storage.Remove(ctx, sector.ID, storiface.FTCache, true, nil); err != nil {
				return nil, xerrors.Errorf("cleaning up cache data: %w", err)
			}
		}

		sb, err := l.executor()
		if err != nil {
			return nil, err
		}

		// lock P1 mutex
		err = l.p1Semaphore.Acquire(context.TODO(), 1)
		if err != nil {
			return nil, err
		}
		defer l.p1Semaphore.Release(1)

		l.counterTask(sealtasks.TTPreCommit1, 1)
		defer l.counterTask(sealtasks.TTPreCommit1, -1)

		return sb.SealPreCommit1(ctx, sector, ticket, pieces)
	})
}

func (l *LocalWorker) SealPreCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.PreCommit1Out) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealPreCommit2, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		l.counterTask(sealtasks.TTPreCommit2, 1)

		defer func() {
			l.counterTask(sealtasks.TTPreCommit2, -1)
		}()

		err := l.p2c2Semaphore.Acquire(context.TODO(), int64(l.c2Count))
		if err != nil {
			return nil, err
		}

		defer l.p2c2Semaphore.Release(int64(l.c2Count))

		return sb.SealPreCommit2(ctx, sector, phase1Out)
	})
}

func (l *LocalWorker) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealCommit1, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
	})
}

func (l *LocalWorker) SealCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.Commit1Out) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealCommit2, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		l.counterTask(sealtasks.TTCommit2, 1)

		defer func() {
			l.counterTask(sealtasks.TTCommit2, -1)
		}()

		err := l.p2c2Semaphore.Acquire(context.TODO(), 1)
		if err != nil {
			return nil, err
		}
		defer l.p2c2Semaphore.Release(1)

		return sb.SealCommit2(ctx, sector, phase1Out)
	})
}

func (l *LocalWorker) ReplicaUpdate(ctx context.Context, sector storage.SectorRef, pieces []abi.PieceInfo) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, ReplicaUpdate, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		sealerOut, err := sb.ReplicaUpdate(ctx, sector, pieces)
		return sealerOut, err
	})
}

func (l *LocalWorker) ProveReplicaUpdate1(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, ProveReplicaUpdate1, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.ProveReplicaUpdate1(ctx, sector, sectorKey, newSealed, newUnsealed)
	})
}

func (l *LocalWorker) ProveReplicaUpdate2(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid, vanillaProofs storage.ReplicaVanillaProofs) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, ProveReplicaUpdate2, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.ProveReplicaUpdate2(ctx, sector, sectorKey, newSealed, newUnsealed, vanillaProofs)
	})
}

func (l *LocalWorker) GenerateSectorKeyFromData(ctx context.Context, sector storage.SectorRef, commD cid.Cid) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, GenerateSectorKey, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return nil, sb.GenerateSectorKeyFromData(ctx, sector, commD)
	})
}

func (l *LocalWorker) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, FinalizeSector, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		if err := sb.FinalizeSector(ctx, sector, keepUnsealed); err != nil {
			return nil, xerrors.Errorf("finalizing sector: %w", err)
		}

		// lingh: sb.FinalizeSector already remove local unsealed sector
		// if len(keepUnsealed) == 0 {
		// 	if err := l.storage.Remove(ctx, sector.ID, storiface.FTUnsealed, true); err != nil {
		// 		return nil, xerrors.Errorf("removing unsealed data: %w", err)
		// 	}
		// }

		// lingh: do move also
		return nil, l.storage.MoveStorage(ctx, sector, storiface.FTCache|storiface.FTSealed)
	})
}

func (l *LocalWorker) FinalizeReplicaUpdate(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, FinalizeReplicaUpdate, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		if err := sb.FinalizeReplicaUpdate(ctx, sector, keepUnsealed); err != nil {
			return nil, xerrors.Errorf("finalizing sector: %w", err)
		}

		if len(keepUnsealed) == 0 {
			if err := l.storage.Remove(ctx, sector.ID, storiface.FTUnsealed, true, nil); err != nil {
				return nil, xerrors.Errorf("removing unsealed data: %w", err)
			}
		}

		return nil, err
	})
}

func (l *LocalWorker) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (storiface.CallID, error) {
	return storiface.UndefCall, xerrors.Errorf("implement me")
}

func (l *LocalWorker) Remove(ctx context.Context, sector abi.SectorID) error {
	var err error

	if rerr := l.storage.Remove(ctx, sector, storiface.FTSealed, true, nil); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, storiface.FTCache, true, nil); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, storiface.FTUnsealed, true, nil); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	return err
}

func (l *LocalWorker) MoveStorage(ctx context.Context, sector storage.SectorRef, types storiface.SectorFileType) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, MoveStorage, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return nil, l.storage.MoveStorage(ctx, sector, types)
	})
}

func (l *LocalWorker) UnsealPiece(ctx context.Context, sector storage.SectorRef, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, cid cid.Cid) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, UnsealPiece, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		log.Debugf("worker will unseal piece now, sector=%+v", sector.ID)
		if err = sb.UnsealPiece(ctx, sector, index, size, randomness, cid); err != nil {
			return nil, xerrors.Errorf("unsealing sector: %w", err)
		}

		if err = l.storage.RemoveCopies(ctx, sector.ID, storiface.FTSealed); err != nil {
			return nil, xerrors.Errorf("removing source data: %w", err)
		}

		if err = l.storage.RemoveCopies(ctx, sector.ID, storiface.FTCache); err != nil {
			return nil, xerrors.Errorf("removing source data: %w", err)
		}

		log.Debugf("worker has unsealed piece, sector=%+v", sector.ID)

		return nil, nil
	})
}

func (l *LocalWorker) TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error) {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	return l.acceptTasks, nil
}

func (l *LocalWorker) TaskDisable(ctx context.Context, tt sealtasks.TaskType) error {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	delete(l.acceptTasks, tt)
	return nil
}

func (l *LocalWorker) TaskEnable(ctx context.Context, tt sealtasks.TaskType) error {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	l.acceptTasks[tt] = struct{}{}
	return nil
}

func (l *LocalWorker) Paths(ctx context.Context) ([]stores.StoragePath, error) {
	return l.localStore.Local(ctx)
}

func (l *LocalWorker) counterTask(tasktype sealtasks.TaskType, c int) {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	count, exist := l.runningTasks[tasktype]
	if !exist {
		l.runningTasks[tasktype] = c
	} else {
		l.runningTasks[tasktype] = count + c
	}
}

func (l *LocalWorker) HasResourceForNewTask(ctx context.Context, tasktype sealtasks.TaskType) bool {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	if l.role == "P2C2" {
		current := l.runningTasks[tasktype]
		max := parallelConfig[tasktype]
		if current >= int(max) {
			return false
		}

		if tasktype == sealtasks.TTPreCommit2 {
			c2count := l.runningTasks[sealtasks.TTCommit2]
			if c2count > 0 {
				return false
			}
		} else if tasktype == sealtasks.TTCommit2 {
			p2count := l.runningTasks[sealtasks.TTPreCommit2]
			if p2count > 0 {
				return false
			}
		}

		return true
	}

	count, exist := l.runningTasks[tasktype]
	if !exist {
		return true
	}

	taskParallelCount := parallelConfig[tasktype]
	if count < int(taskParallelCount) {
		return true
	}

	return false
}

func (l *LocalWorker) Info(context.Context) (storiface.WorkerInfo, error) {
	hostname, err := os.Hostname() // TODO: allow overriding from config
	if err != nil {
		panic(err)
	}

	// gpus, err := ffi.GetGPUDevices()
	// if err != nil {
	// 	log.Errorf("getting gpu devices failed: %+v", err)
	// }

	// h, err := sysinfo.Host()
	// if err != nil {
	// 	return storiface.WorkerInfo{}, xerrors.Errorf("getting host info: %w", err)
	// }

	// mem, err := h.Memory()
	// if err != nil {
	// 	return storiface.WorkerInfo{}, xerrors.Errorf("getting memory info: %w", err)
	// }

	// memSwap := mem.VirtualTotal
	// if l.noSwap {
	// 	memSwap = 0
	// }

	res := l.getWorkerResourceConfig()

	return storiface.WorkerInfo{
		GroupID:   l.groupID,
		Hostname:  hostname,
		Resources: res,
	}, nil
}

var parallelConfig = map[sealtasks.TaskType]uint32{
	sealtasks.TTAddPiece:   1,
	sealtasks.TTCommit1:    8,
	sealtasks.TTCommit2:    1,
	sealtasks.TTPreCommit1: 1,
	sealtasks.TTPreCommit2: 1,
	sealtasks.TTFinalize:   1,
}

func (l *LocalWorker) getWorkerResourceConfig() storiface.WorkerResources {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()
	res := storiface.WorkerResources{}
	for k := range l.acceptTasks {
		kk, _ := parallelConfig[k]
		switch k {
		case sealtasks.TTAddPiece:
			res.AP = kk
		case sealtasks.TTCommit1:
			res.C1 = kk
		case sealtasks.TTCommit2:
			res.C2 = kk
		case sealtasks.TTPreCommit1:
			res.P1 = kk
		case sealtasks.TTPreCommit2:
			res.P2 = kk
		case sealtasks.TTFinalize:
			res.FIN = kk
		}
	}

	return res
}

func (l *LocalWorker) Session(ctx context.Context) (uuid.UUID, error) {
	if atomic.LoadInt64(&l.testDisable) == 1 {
		return uuid.UUID{}, xerrors.Errorf("disabled")
	}

	select {
	case <-l.closing:
		return ClosedWorkerID, nil
	default:
		return l.session, nil
	}
}

func (l *LocalWorker) Close() error {
	close(l.chanCacheClear)
	close(l.closing)
	return nil
}

// WaitQuiet blocks as long as there are tasks running
func (l *LocalWorker) WaitQuiet() {
	l.running.Wait()
}

type wctx struct {
	vals    context.Context
	closing chan struct{}
}

func (w *wctx) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (w *wctx) Done() <-chan struct{} {
	return w.closing
}

func (w *wctx) Err() error {
	select {
	case <-w.closing:
		return context.Canceled
	default:
		return nil
	}
}

func (w *wctx) Value(key interface{}) interface{} {
	return w.vals.Value(key)
}

var _ context.Context = &wctx{}

var _ Worker = &LocalWorker{}
