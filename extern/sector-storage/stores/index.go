package stores

import (
	"context"
	"errors"

	"fmt"
	"math/rand"

	"net/url"
	gopath "path"
	"sort"
	"sync"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/metrics"
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

	StorageList(ctx context.Context) (map[ID][]Decl, error)

	TryBindSector2SealStorage(ctx context.Context, fileType storiface.SectorFileType, pathType storiface.PathType, sector abi.SectorID, groupID string) (StorageInfo, error)
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

func (i *Index) allocStorageForFinalize(ctx context.Context, sector abi.SectorID, ft storiface.SectorFileType) (StorageInfo, error) {
	log.Debugf("allocStorageForFinalize: sector %s, ft:%d", sector, ft)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache
	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := Decl{sector, fileType}
		dd, exist := i.sectors[d]
		if exist {
			for _, d := range dd {
				s, exist := i.stores[d.storage]
				if exist {
					log.Debugf("allocStorageForFinalize found sector: %d bind to storage %s, path:%v , filetype:%d, return it",
						sector, s.info.ID, s.info.URLs, ft)

					return *s.info, nil
				}
			}
		}
	}

	var candidates []*storageEntry
	for _, p := range i.stores {
		// only bind to sealing storage
		if !p.info.CanStore {
			///log.Infof("allocStorageForFinalize storage %s not a store storage",
			//p.info.ID)
			continue
		}

		// readonly storage
		if p.info.Weight == 0 {
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
	candidate := candidateSelect(candidates)
	if candidate.info.MaxSealingSectors != 0 {
		candidate.bindSectors[sector] = struct{}{}
	}

	// err := i.StorageDeclareSector(ctx, storageID, sector, ft, true)
	// if err != nil {
	// 	return StorageInfo{}, err
	// }
	log.Debugf("allocStorageForFinalize bind ok: sector %s, storage ID:%s", sector, candidate.info.ID)
	return *candidate.info, nil
}

func candidateSelect(candidates []*storageEntry) *storageEntry {
	var weightSum int = 0
	for _, s := range candidates {
		weightSum = weightSum + int(s.info.Weight)
	}

	v := rand.Int() % weightSum
	start := 0
	for _, s := range candidates {
		r1 := start
		r2 := start + int(s.info.Weight)
		if v >= r1 && v < r2 {
			return s
		}

		start = r2
	}

	log.Warnf("candidateSelect: random match failed, return first storage:%v", candidates[0].info.URLs)
	return candidates[0]
}

func (i *Index) TryBindSector2SealStorage(ctx context.Context, fileType storiface.SectorFileType, pathType storiface.PathType,
	sector abi.SectorID, groupID string) (StorageInfo, error) {
	log.Debugf("TryBindSector2SealStorage: %s, groupID:%s", sector, groupID)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache
	i.lk.Lock()
	defer i.lk.Unlock()

	if pathType == storiface.PathStorage {
		log.Debugf("TryBindSector2SealStorage worker pathType is PathStorage, sector:%s, return a storage path", sector)

		// just continue
		return i.allocStorageForFinalize(ctx, sector, fileType)
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
	log.Debugf("TryBindSector2SealStorage bind ok: sector %s, storage ID:%s", sector, candidate.info.ID)
	return *candidate.info, nil
}

func (i *Index) UnBindSector2SealStorage(ctx context.Context, sector abi.SectorID) error {
	log.Debugf("UnBindSector2SealStorage: %s", sector)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache

	i.lk.Lock()
	defer i.lk.Unlock()

	for _, p := range i.stores {
		_, ok := p.bindSectors[sector]
		if ok {
			delete(p.bindSectors, sector)
			log.Debugf("UnBindSector2SealStorage ok: sector %s, storage ID:%s", sector, p.info.ID)
			return nil
		}
	}

	log.Debugf("UnBindSector2SealStorage ok: sector %s not yet bind to any storage", sector)
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
			mm, ok := byID[id.storage]
			if ok {
				mm[decl.SectorID] |= decl.SectorFileType
			}
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

		// uloop:
		// 	for _, u := range si.URLs {
		// 		for _, l := range i.stores[si.ID].info.URLs {
		// 			if u == l {
		// 				continue uloop
		// 			}
		// 		}

		// 		i.stores[si.ID].info.URLs = append(i.stores[si.ID].info.URLs, u)
		// 	}

		i.stores[si.ID].info.URLs = si.URLs
		i.stores[si.ID].info.Weight = si.Weight
		i.stores[si.ID].info.MaxStorage = si.MaxStorage
		i.stores[si.ID].info.CanSeal = si.CanSeal
		i.stores[si.ID].info.CanStore = si.CanStore
		i.stores[si.ID].info.GroupID = si.GroupID
		i.stores[si.ID].info.MaxSealingSectors = si.MaxSealingSectors
		// clear bind sectors
		i.stores[si.ID].bindSectors = make(map[abi.SectorID]struct{})

		return nil
	}

	i.stores[si.ID] = &storageEntry{
		info:          &si,
		fsi:           st,
		bindSectors:   make(map[abi.SectorID]struct{}),
		lastHeartbeat: time.Now(),
	}

	// remove all decl made by this storage
	// sectors := make(map[Decl]struct{})
	// for decl, ids := range i.sectors {
	// 	found := false
	// 	for _, id := range ids {
	// 		if id.storage == si.ID {
	// 			found = true
	// 			break
	// 		}
	// 	}

	// 	if found {
	// 		sectors[decl] = struct{}{}
	// 	}
	// }

	// if len(sectors) > 0 {
	// 	for k := range sectors {
	// 		delete(i.sectors, k)
	// 	}
	// }

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

	if report.Stat.Capacity > 0 {
		ctx, _ = tag.New(ctx, tag.Upsert(metrics.StorageID, string(id)))

		stats.Record(ctx, metrics.StorageFSAvailable.M(float64(report.Stat.FSAvailable)/float64(report.Stat.Capacity)))
		stats.Record(ctx, metrics.StorageAvailable.M(float64(report.Stat.Available)/float64(report.Stat.Capacity)))
		stats.Record(ctx, metrics.StorageReserved.M(float64(report.Stat.Reserved)/float64(report.Stat.Capacity)))

		stats.Record(ctx, metrics.StorageCapacityBytes.M(report.Stat.Capacity))
		stats.Record(ctx, metrics.StorageFSAvailableBytes.M(report.Stat.FSAvailable))
		stats.Record(ctx, metrics.StorageAvailableBytes.M(report.Stat.Available))
		stats.Record(ctx, metrics.StorageReservedBytes.M(report.Stat.Reserved))

		if report.Stat.Max > 0 {
			stats.Record(ctx, metrics.StorageLimitUsed.M(float64(report.Stat.Used)/float64(report.Stat.Max)))
			stats.Record(ctx, metrics.StorageLimitUsedBytes.M(report.Stat.Used))
			stats.Record(ctx, metrics.StorageLimitMaxBytes.M(report.Stat.Max))
		}
	}

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
		var arr = i.sectors[d]

		for i, sid := range arr {
			if sid.storage == storageID {
				if !sid.primary && primary {
					sid.primary = true
				} else {
					log.Warnf("sector %v redeclared in %s", s, storageID)
				}

				if i != 0 {
					arr[0], arr[i] = arr[i], arr[0]
				}

				continue loop
			}
		}

		var meta = &declMeta{
			storage: storageID,
			primary: primary,
		}

		var arr2 = make([]*declMeta, len(arr)+1)
		arr2[0] = meta
		if len(arr) > 0 {
			copy(arr2[1:], arr)
		}
		i.sectors[d] = arr2
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

		urls := make([]string, 0, len(st.info.URLs))
		for _, u := range st.info.URLs {
			rl, err := url.Parse(u)
			if err != nil {
				log.Errorf("StorageFindSector failed to parse url: %w", err)
				continue
			}

			rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
			urls = append(urls, rl.String())
		}

		if len(urls) < 1 {
			log.Errorf("StorageFindSector find sector %v failed, storage id:%s has no url to access", s, id)
			continue
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

	// if allowFetch {
	// 	spaceReq, err := ft.SealSpaceUse(ssize)
	// 	if err != nil {
	// 		return nil, xerrors.Errorf("estimating required space: %w", err)
	// 	}

	// 	for id, st := range i.stores {
	// 		if !st.info.CanSeal {
	// 			continue
	// 		}

	// 		if spaceReq > uint64(st.fsi.Available) {
	// 			log.Debugf("not selecting on %s, out of space (available: %d, need: %d)", st.info.ID, st.fsi.Available, spaceReq)
	// 			continue
	// 		}

	// 		if time.Since(st.lastHeartbeat) > SkippedHeartbeatThresh {
	// 			log.Debugf("not selecting on %s, didn't receive heartbeats for %s", st.info.ID, time.Since(st.lastHeartbeat))
	// 			continue
	// 		}

	// 		if st.heartbeatErr != nil {
	// 			log.Debugf("not selecting on %s, heartbeat error: %s", st.info.ID, st.heartbeatErr)
	// 			continue
	// 		}

	// 		if _, ok := storageIDs[id]; ok {
	// 			continue
	// 		}

	// 		urls := make([]string, len(st.info.URLs))
	// 		for k, u := range st.info.URLs {
	// 			rl, err := url.Parse(u)
	// 			if err != nil {
	// 				return nil, xerrors.Errorf("failed to parse url: %w", err)
	// 			}

	// 			rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
	// 			urls[k] = rl.String()
	// 		}

	// 		out = append(out, SectorStorageInfo{
	// 			GroupID:           st.info.GroupID,
	// 			MaxSealingSectors: st.info.MaxSealingSectors,
	// 			ID:                id,
	// 			URLs:              urls,
	// 			Weight:            st.info.Weight * 0, // TODO: something better than just '0'

	// 			CanSeal:  st.info.CanSeal,
	// 			CanStore: st.info.CanStore,

	// 			Primary: false,
	// 		})
	// 	}
	// }

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

	var err error
	var spaceReq uint64
	switch pathType {
	case storiface.PathSealing:
		spaceReq, err = allocate.SealSpaceUse(ssize)
	case storiface.PathStorage:
		spaceReq, err = allocate.StoreSpaceUse(ssize)
	default:
		panic(fmt.Sprintf("unexpected pathType: %s", pathType))
	}
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
		log.Debugf("StorageBestAlloc, no good path found found")
		return nil, nil
	}

	sort.Slice(candidates, func(i, j int) bool {

		iw := len(candidates[i].bindSectors)
		jw := len(candidates[j].bindSectors)

		if iw != jw {
			return iw < jw
		}

		iiw := big.Mul(big.NewInt(candidates[i].fsi.Available), big.NewInt(int64(candidates[i].info.Weight)))
		jjw := big.Mul(big.NewInt(candidates[j].fsi.Available), big.NewInt(int64(candidates[j].info.Weight)))

		return iiw.GreaterThan(jjw)
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
