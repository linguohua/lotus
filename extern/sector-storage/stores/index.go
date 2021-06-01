package stores

import (
	"context"
	"errors"
	"math/rand"
	"net/url"
	gopath "path"
	"sort"
	"sync"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

var HeartbeatInterval = 10 * time.Second
var SkippedHeartbeatThresh = HeartbeatInterval * 5

// ID identifies sector storage by UUID. One sector storage should map to one
//  filesystem, local or networked / shared by multiple machines
type ID string

type StorageInfo struct {
	GroupID           string
	MaxSealingSectors int

	ID         ID
	URLs       []string // TODO: Support non-http transports
	Weight     uint64
	MaxStorage uint64

	CanSeal  bool
	CanStore bool
}

type HealthReport struct {
	Stat fsutil.FsStat
	Err  string
}

type SectorStorageInfo struct {
	GroupID           string
	MaxSealingSectors int

	ID     ID
	URLs   []string // TODO: Support non-http transports
	Weight uint64

	CanSeal  bool
	CanStore bool

	Primary bool
}

type SectorIndex interface { // part of storage-miner api
	StorageAttach(context.Context, StorageInfo, fsutil.FsStat) error
	StorageInfo(context.Context, ID) (StorageInfo, error)
	StorageReportHealth(context.Context, ID, HealthReport) error

	StorageDeclareSector(ctx context.Context, storageID ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error
	StorageDropSector(ctx context.Context, storageID ID, s abi.SectorID, ft storiface.SectorFileType) error
	StorageFindSector(ctx context.Context, sector abi.SectorID, ft storiface.SectorFileType, ssize abi.SectorSize, allowFetch bool) ([]SectorStorageInfo, error)

	StorageBestAlloc(ctx context.Context, allocate storiface.SectorFileType, ssize abi.SectorSize, pathType storiface.PathType) ([]StorageInfo, error)

	// atomically acquire locks on all sector file types. close ctx to unlock
	StorageLock(ctx context.Context, sector abi.SectorID, read storiface.SectorFileType, write storiface.SectorFileType) error
	StorageTryLock(ctx context.Context, sector abi.SectorID, read storiface.SectorFileType, write storiface.SectorFileType) (bool, error)

	TryBindSector2SealStorage(ctx context.Context, sector abi.SectorID, groupID string) (StorageInfo, error)
	UnBindSector2SealStorage(ctx context.Context, sector abi.SectorID) error
}

type Decl struct {
	abi.SectorID
	storiface.SectorFileType
}

type declMeta struct {
	storage ID
	primary bool
}

type storageEntry struct {
	info *StorageInfo
	fsi  fsutil.FsStat

	lastHeartbeat time.Time
	heartbeatErr  error

	bindSectors map[abi.SectorID]struct{}
}

type Index struct {
	*indexLocks
	lk sync.RWMutex

	sectors map[Decl][]*declMeta
	stores  map[ID]*storageEntry
}

func NewIndex() *Index {
	return &Index{
		indexLocks: &indexLocks{
			locks: map[abi.SectorID]*sectorLock{},
		},
		sectors: map[Decl][]*declMeta{},
		stores:  map[ID]*storageEntry{},
	}
}

func (i *Index) allocStorageForFinalize(ctx context.Context, sector abi.SectorID) (StorageInfo, error) {
	log.Infof("allocStorageForFinalize: sector %s", sector)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache
	i.lk.RLock()
	defer i.lk.RUnlock()

	var candidates []*storageEntry
	for _, p := range i.stores {
		// only bind to sealing storage
		if !p.info.CanStore {
			///log.Infof("allocStorageForFinalize storage %s not a store storage",
			//p.info.ID)
			continue
		}

		if time.Since(p.lastHeartbeat) > SkippedHeartbeatThresh {
			log.Debugf("allocStorageForFinalize not allocating on %s, didn't receive heartbeats for %s",
				p.info.ID, time.Since(p.lastHeartbeat))
			continue
		}

		if p.heartbeatErr != nil {
			log.Debugf("allocStorageForFinalize not allocating on %s, heartbeat error: %s",
				p.info.ID, p.heartbeatErr)
			continue
		}

		candidates = append(candidates, p)
	}

	if len(candidates) < 1 {
		return StorageInfo{}, xerrors.Errorf("allocStorageForFinalize failed to found storage to bind %s",
			sector)
	}

	// random select one
	candidate := candidates[rand.Int()%len(candidates)]
	if candidate.info.MaxSealingSectors != 0 {
		candidate.bindSectors[sector] = struct{}{}
	}

	// err := i.StorageDeclareSector(ctx, storageID, sector, ft, true)
	// if err != nil {
	// 	return StorageInfo{}, err
	// }
	log.Infof("allocStorageForFinalize bind ok: sector %s, storage ID:%s", sector, candidate.info.ID)
	return *candidate.info, nil
}

func (i *Index) TryBindSector2SealStorage(ctx context.Context, sector abi.SectorID, groupID string) (StorageInfo, error) {
	log.Infof("TryBindSector2SealStorage: %s, groupID:%s", sector, groupID)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache
	i.lk.RLock()
	defer i.lk.RUnlock()

	if groupID == "" {
		log.Errorf("TryBindSector2SealStorage worker GroupID is empty, sector:%s, return a storage path", sector)

		// just continue
		return i.allocStorageForFinalize(ctx, sector)
	}

	var candidates []*storageEntry
	for _, p := range i.stores {
		// only bind to sealing storage
		if !p.info.CanSeal {
			// log.Infof("TryBindSector2SealStorage storage %s not a seal storage",
			// 	p.info.ID)
			continue
		}

		if p.info.GroupID == "" {
			// log.Errorf("TryBindSector2SealStorage storage %s is seal storage but without GroupID",
			// 	p.info.ID)
			continue
		}

		if p.info.GroupID != groupID {
			// log.Infof("TryBindSector2SealStorage group not match, require:%s, p:%s",
			// 	groupID, p.info.GroupID)
			continue
		}

		if p.info.MaxSealingSectors != 0 && len(p.bindSectors) >= p.info.MaxSealingSectors {
			_, ok := p.bindSectors[sector]
			if ok {
				// log.Infof("TryBindSector2SealStorage bind ok, already bind: sector %s, storage ID:%s",
				// 	sector, p.info.ID)
				return *p.info, nil
			}

			// log.Infof("TryBindSector2SealStorage storage %s already bind to sector:%s",
			// 	p.info.ID, p.bindSector)
			continue
		}

		if time.Since(p.lastHeartbeat) > SkippedHeartbeatThresh {
			log.Debugf("TryBindSector2SealStorage not allocating on %s, didn't receive heartbeats for %s",
				p.info.ID, time.Since(p.lastHeartbeat))
			continue
		}

		if p.heartbeatErr != nil {
			log.Debugf("TryBindSector2SealStorage not allocating on %s, heartbeat error: %s",
				p.info.ID, p.heartbeatErr)
			continue
		}

		candidates = append(candidates, p)
	}

	if len(candidates) < 1 {
		return StorageInfo{}, xerrors.Errorf("TryBindSector2SealStorage failed to found storage to bind %s, groupID:%s",
			sector, groupID)
	}

	// random select one
	candidate := candidates[rand.Int()%len(candidates)]
	if candidate.info.MaxSealingSectors != 0 {
		candidate.bindSectors[sector] = struct{}{}
	}

	// err := i.StorageDeclareSector(ctx, storageID, sector, ft, true)
	// if err != nil {
	// 	return StorageInfo{}, err
	// }
	log.Infof("TryBindSector2SealStorage bind ok: sector %s, storage ID:%s", sector, candidate.info.ID)
	return *candidate.info, nil
}

func (i *Index) UnBindSector2SealStorage(ctx context.Context, sector abi.SectorID) error {
	log.Infof("UnBindSector2SealStorage: %s", sector)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache

	i.lk.RLock()
	defer i.lk.RUnlock()

	for _, p := range i.stores {
		_, ok := p.bindSectors[sector]
		if ok {
			delete(p.bindSectors, sector)
			log.Infof("UnBindSector2SealStorage ok: sector %s, storage ID:%s", sector, p.info.ID)
			return nil
		}
	}

	log.Infof("UnBindSector2SealStorage ok: sector %s not yet bind to any storage", sector)
	return nil
}

func (i *Index) StorageList(ctx context.Context) (map[ID][]Decl, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	byID := map[ID]map[abi.SectorID]storiface.SectorFileType{}

	for id := range i.stores {
		byID[id] = map[abi.SectorID]storiface.SectorFileType{}
	}
	for decl, ids := range i.sectors {
		for _, id := range ids {
			byID[id.storage][decl.SectorID] |= decl.SectorFileType
		}
	}

	out := map[ID][]Decl{}
	for id, m := range byID {
		out[id] = []Decl{}
		for sectorID, fileType := range m {
			out[id] = append(out[id], Decl{
				SectorID:       sectorID,
				SectorFileType: fileType,
			})
		}
	}

	return out, nil
}

func (i *Index) StorageAttach(ctx context.Context, si StorageInfo, st fsutil.FsStat) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	log.Infof("StorageAttach, New sector storage: %+v", si)

	if _, ok := i.stores[si.ID]; ok {
		for _, u := range si.URLs {
			if _, err := url.Parse(u); err != nil {
				return xerrors.Errorf("failed to parse url %s: %w", si.URLs, err)
			}
		}

	uloop:
		for _, u := range si.URLs {
			for _, l := range i.stores[si.ID].info.URLs {
				if u == l {
					continue uloop
				}
			}

			i.stores[si.ID].info.URLs = append(i.stores[si.ID].info.URLs, u)
		}

		i.stores[si.ID].info.Weight = si.Weight
		i.stores[si.ID].info.MaxStorage = si.MaxStorage
		i.stores[si.ID].info.CanSeal = si.CanSeal
		i.stores[si.ID].info.CanStore = si.CanStore
		i.stores[si.ID].info.GroupID = si.GroupID
		i.stores[si.ID].info.MaxSealingSectors = si.MaxSealingSectors

		return nil
	}
	i.stores[si.ID] = &storageEntry{
		info:          &si,
		fsi:           st,
		bindSectors:   make(map[abi.SectorID]struct{}),
		lastHeartbeat: time.Now(),
	}
	return nil
}

func (i *Index) StorageReportHealth(ctx context.Context, id ID, report HealthReport) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	ent, ok := i.stores[id]
	if !ok {
		return xerrors.Errorf("health report for unknown storage: %s", id)
	}

	ent.fsi = report.Stat
	if report.Err != "" {
		ent.heartbeatErr = errors.New(report.Err)
	} else {
		ent.heartbeatErr = nil
	}
	ent.lastHeartbeat = time.Now()

	return nil
}

func (i *Index) StorageDeclareSector(ctx context.Context, storageID ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error {
	i.lk.Lock()
	defer i.lk.Unlock()

loop:
	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := Decl{s, fileType}

		for _, sid := range i.sectors[d] {
			if sid.storage == storageID {
				if !sid.primary && primary {
					sid.primary = true
				} else {
					log.Warnf("sector %v redeclared in %s", s, storageID)
				}
				continue loop
			}
		}

		i.sectors[d] = append(i.sectors[d], &declMeta{
			storage: storageID,
			primary: primary,
		})
	}

	store, exist := i.stores[storageID]
	if exist && store.info.GroupID != "" {
		// NOTE: store.info.GroupID != "" means store.info.MaxSealingSectors > 0
		_, ok := store.bindSectors[s]
		if ok {
			log.Infof("sector %v declared in %s, store already bind to it", s, storageID)
		} else {
			if len(store.bindSectors) < store.info.MaxSealingSectors {
				store.bindSectors[s] = struct{}{}
				log.Infof("sector %v declared in %s, re-bind store to sector", s, storageID)
			} else {
				log.Errorf("sector %v declared in %s, but store has bind fulled",
					s, storageID)
			}
		}
	}

	return nil
}

func (i *Index) StorageDropSector(ctx context.Context, storageID ID, s abi.SectorID, ft storiface.SectorFileType) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := Decl{s, fileType}

		if len(i.sectors[d]) == 0 {
			continue
		}

		rewritten := make([]*declMeta, 0, len(i.sectors[d])-1)
		for _, sid := range i.sectors[d] {
			if sid.storage == storageID {
				continue
			}

			rewritten = append(rewritten, sid)
		}
		if len(rewritten) == 0 {
			delete(i.sectors, d)
			continue
		}

		i.sectors[d] = rewritten
	}

	// only remove bind when release sealed
	if ft == storiface.FTSealed {
		store, exist := i.stores[storageID]
		if exist && store.info.GroupID != "" {
			// NOTE: store.info.GroupID != "" means store.info.MaxSealingSectors > 0
			_, ok := store.bindSectors[s]
			if ok {
				log.Infof("sector %v drop in %s, unbind",
					s, storageID)
				delete(store.bindSectors, s)
			} else {
				log.Infof("sector %v drop in %s, but store has not bind to it",
					s, storageID)
			}
		}
	}

	return nil
}

func (i *Index) StorageFindSector(ctx context.Context, s abi.SectorID, ft storiface.SectorFileType, ssize abi.SectorSize, allowFetch bool) ([]SectorStorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	storageIDs := map[ID]uint64{}
	isprimary := map[ID]bool{}

	for _, pathType := range storiface.PathTypes {
		if ft&pathType == 0 {
			continue
		}

		for _, id := range i.sectors[Decl{s, pathType}] {
			storageIDs[id.storage]++
			isprimary[id.storage] = isprimary[id.storage] || id.primary
		}
	}

	out := make([]SectorStorageInfo, 0, len(storageIDs))

	for id, n := range storageIDs {
		st, ok := i.stores[id]
		if !ok {
			log.Warnf("storage %s is not present in sector index (referenced by sector %v)", id, s)
			continue
		}

		urls := make([]string, len(st.info.URLs))
		for k, u := range st.info.URLs {
			rl, err := url.Parse(u)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse url: %w", err)
			}

			rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
			urls[k] = rl.String()
		}

		out = append(out, SectorStorageInfo{
			GroupID:           st.info.GroupID,
			MaxSealingSectors: st.info.MaxSealingSectors,
			ID:                id,
			URLs:              urls,
			Weight:            st.info.Weight * n, // storage with more sector types is better

			CanSeal:  st.info.CanSeal,
			CanStore: st.info.CanStore,

			Primary: isprimary[id],
		})
	}

	if allowFetch {
		spaceReq, err := ft.SealSpaceUse(ssize)
		if err != nil {
			return nil, xerrors.Errorf("estimating required space: %w", err)
		}

		for id, st := range i.stores {
			if !st.info.CanSeal {
				continue
			}

			if spaceReq > uint64(st.fsi.Available) {
				log.Debugf("not selecting on %s, out of space (available: %d, need: %d)", st.info.ID, st.fsi.Available, spaceReq)
				continue
			}

			if time.Since(st.lastHeartbeat) > SkippedHeartbeatThresh {
				log.Debugf("not selecting on %s, didn't receive heartbeats for %s", st.info.ID, time.Since(st.lastHeartbeat))
				continue
			}

			if st.heartbeatErr != nil {
				log.Debugf("not selecting on %s, heartbeat error: %s", st.info.ID, st.heartbeatErr)
				continue
			}

			if _, ok := storageIDs[id]; ok {
				continue
			}

			urls := make([]string, len(st.info.URLs))
			for k, u := range st.info.URLs {
				rl, err := url.Parse(u)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse url: %w", err)
				}

				rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
				urls[k] = rl.String()
			}

			out = append(out, SectorStorageInfo{
				GroupID:           st.info.GroupID,
				MaxSealingSectors: st.info.MaxSealingSectors,
				ID:                id,
				URLs:              urls,
				Weight:            st.info.Weight * 0, // TODO: something better than just '0'

				CanSeal:  st.info.CanSeal,
				CanStore: st.info.CanStore,

				Primary: false,
			})
		}
	}

	return out, nil
}

func (i *Index) StorageInfo(ctx context.Context, id ID) (StorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	si, found := i.stores[id]
	if !found {
		return StorageInfo{}, xerrors.Errorf("sector store not found")
	}

	return *si.info, nil
}

func (i *Index) StorageBestAlloc(ctx context.Context, allocate storiface.SectorFileType, ssize abi.SectorSize, pathType storiface.PathType) ([]StorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	var candidates []*storageEntry

	spaceReq, err := allocate.SealSpaceUse(ssize)
	if err != nil {
		return nil, xerrors.Errorf("estimating required space: %w", err)
	}

	for _, p := range i.stores {
		if (pathType == storiface.PathSealing) && !p.info.CanSeal {
			continue
		}
		if (pathType == storiface.PathStorage) && !p.info.CanStore {
			continue
		}

		if p.info.MaxSealingSectors != 0 && len(p.bindSectors) >= p.info.MaxSealingSectors {
			//log.Debugf("not allocating on %s, it already bind full", p.info.ID)
			continue
		}

		if spaceReq > uint64(p.fsi.Available) {
			log.Debugf("not allocating on %s, out of space (available: %d, need: %d)", p.info.ID, p.fsi.Available, spaceReq)
			continue
		}

		if time.Since(p.lastHeartbeat) > SkippedHeartbeatThresh {
			log.Debugf("not allocating on %s, didn't receive heartbeats for %s", p.info.ID, time.Since(p.lastHeartbeat))
			continue
		}

		if p.heartbeatErr != nil {
			log.Debugf("not allocating on %s, heartbeat error: %s", p.info.ID, p.heartbeatErr)
			continue
		}

		candidates = append(candidates, p)
	}

	if len(candidates) == 0 {
		return nil, xerrors.New("no good path found")
	}

	sort.Slice(candidates, func(i, j int) bool {
		iw := big.Mul(big.NewInt(candidates[i].fsi.Available), big.NewInt(int64(candidates[i].info.Weight)))
		jw := big.Mul(big.NewInt(candidates[j].fsi.Available), big.NewInt(int64(candidates[j].info.Weight)))

		return iw.GreaterThan(jw)
	})

	out := make([]StorageInfo, len(candidates))
	for i, candidate := range candidates {
		out[i] = *candidate.info
	}

	return out, nil
}

func (i *Index) FindSector(id abi.SectorID, typ storiface.SectorFileType) ([]ID, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	f, ok := i.sectors[Decl{
		SectorID:       id,
		SectorFileType: typ,
	}]
	if !ok {
		return nil, nil
	}
	out := make([]ID, 0, len(f))
	for _, meta := range f {
		out = append(out, meta.storage)
	}

	return out, nil
}

var _ SectorIndex = &Index{}
