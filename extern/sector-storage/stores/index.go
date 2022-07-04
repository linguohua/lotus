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

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/index.go -package=mocks . SectorIndex

type SectorIndex interface { // part of storage-miner api
	StorageAttach(context.Context, storiface.StorageInfo, fsutil.FsStat) error
	StorageInfo(context.Context, storiface.ID) (storiface.StorageInfo, error)
	StorageReportHealth(context.Context, storiface.ID, storiface.HealthReport) error

	StorageDeclareSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error
	StorageDropSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType) error
	StorageFindSector(ctx context.Context, sector abi.SectorID, ft storiface.SectorFileType, ssize abi.SectorSize, allowFetch bool) ([]storiface.SectorStorageInfo, error)

	StorageBestAlloc(ctx context.Context, allocate storiface.SectorFileType, ssize abi.SectorSize, pathType storiface.PathType) ([]storiface.StorageInfo, error)

	// atomically acquire locks on all sector file types. close ctx to unlock
	StorageLock(ctx context.Context, sector abi.SectorID, read storiface.SectorFileType, write storiface.SectorFileType) error
	StorageTryLock(ctx context.Context, sector abi.SectorID, read storiface.SectorFileType, write storiface.SectorFileType) (bool, error)
	StorageGetLocks(ctx context.Context) (storiface.SectorLocks, error)

	StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error)
	TryBindSector2SealStorage(ctx context.Context, fileType storiface.SectorFileType, pathType storiface.PathType, sector abi.SectorID, groupID string) ([]storiface.StorageInfo, error)
	UnBindSector2SealStorage(ctx context.Context, sector abi.SectorID) error
}

type declMeta struct {
	storage storiface.ID
	primary bool
}

type storageEntry struct {
	info *storiface.StorageInfo
	fsi  fsutil.FsStat

	lastHeartbeat time.Time
	heartbeatErr  error

	bindSectors map[abi.SectorID]struct{}
}

type Index struct {
	*indexLocks
	lk sync.RWMutex

	sectors        map[storiface.Decl][]*declMeta
	stores         map[storiface.ID]*storageEntry
	sectorSizeMemo uint64
}

func NewIndex() *Index {
	return &Index{
		indexLocks: &indexLocks{
			locks: map[abi.SectorID]*sectorLock{},
		},
		sectors: map[storiface.Decl][]*declMeta{},
		stores:  map[storiface.ID]*storageEntry{},
	}
}

func (i *Index) allocStorageForFinalize(sector abi.SectorID, ft storiface.SectorFileType) ([]storiface.StorageInfo, error) {
	log.Debugf("allocStorageForFinalize: sector %s, ft:%d", sector, ft)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache
	ss := make([]storiface.StorageInfo, len(storiface.PathTypes))
	found := false
	for j, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := storiface.Decl{sector, fileType}
		dd, exist := i.sectors[d]
		if exist {
			for _, d := range dd {
				s, exist := i.stores[d.storage]
				if exist {
					log.Debugf("allocStorageForFinalize found sector: %d bind to storage %s, path:%v , filetype:%d, return it",
						sector, s.info.ID, s.info.URLs, ft)

					// already allocated
					ss[j] = *s.info
					found = true
					break
				}
			}
		}
	}

	// already found
	if found {
		return ss, nil
	}

	var spaceReq uint64 = i.sectorSizeMemo * 2
	// allocate for new sector to finalize to storage
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

		// user set limit
		if p.info.MaxSealingSectors > 0 && len(p.bindSectors) >= p.info.MaxSealingSectors {
			log.Debugf("allocStorageForFinalize not allocating on %s,  sector count exceed MaxSealingSectors %d",
				p.info.ID, p.info.MaxSealingSectors)
			continue
		}

		if spaceReq > uint64(p.fsi.Available) {
			log.Debugf("not allocating on %s, out of space (available: %d, need: %d)", p.info.ID, p.fsi.Available, spaceReq)
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
		return nil, xerrors.Errorf("allocStorageForFinalize failed to found storage to bind %s",
			sector)
	}

	// random select one
	candidate := candidateSelect(candidates)
	if candidate.info.MaxSealingSectors > 0 {
		candidate.bindSectors[sector] = struct{}{}
	}

	// err := i.StorageDeclareSector(ctx, storageID, sector, ft, true)
	// if err != nil {
	// 	return StorageInfo{}, err
	// }
	log.Debugf("allocStorageForFinalize bind ok: sector %s, storage ID:%s", sector, candidate.info.ID)
	return []storiface.StorageInfo{*candidate.info}, nil
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
	sector abi.SectorID, groupID string) ([]storiface.StorageInfo, error) {
	log.Debugf("TryBindSector2SealStorage: %s, groupID:%s", sector, groupID)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache
	i.lk.Lock()
	defer i.lk.Unlock()

	if pathType == storiface.PathStorage {
		log.Debugf("TryBindSector2SealStorage worker pathType is PathStorage, sector:%s, return a storage path", sector)

		// just continue
		return i.allocStorageForFinalize(sector, fileType)
	}

	return i.allocStorageForSealing(sector, groupID)
}

func (i *Index) allocStorageForSealing(sector abi.SectorID, groupID string) ([]storiface.StorageInfo, error) {
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

		if p.info.MaxSealingSectors > 0 && len(p.bindSectors) >= p.info.MaxSealingSectors {
			_, ok := p.bindSectors[sector]
			if ok {
				// log.Infof("TryBindSector2SealStorage bind ok, already bind: sector %s, storage ID:%s",
				// 	sector, p.info.ID)
				return []storiface.StorageInfo{*p.info}, nil
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
		return nil, xerrors.Errorf("TryBindSector2SealStorage failed to found storage to bind %s, groupID:%s",
			sector, groupID)
	}

	// random select one
	candidate := candidates[rand.Int()%len(candidates)]
	if candidate.info.MaxSealingSectors > 0 {
		// early bind
		candidate.bindSectors[sector] = struct{}{}
	}

	// err := i.StorageDeclareSector(ctx, storageID, sector, ft, true)
	// if err != nil {
	// 	return StorageInfo{}, err
	// }
	log.Debugf("TryBindSector2SealStorage bind ok: sector %s, storage ID:%s", sector, candidate.info.ID)
	return []storiface.StorageInfo{*candidate.info}, nil
}

func (i *Index) UnBindSector2SealStorage(ctx context.Context, sector abi.SectorID) error {
	log.Errorf("UnBindSector2SealStorage: %s is not implemented", sector)
	// ft := storiface.FTUnsealed | storiface.FTSealed | storiface.FTCache

	// i.lk.Lock()
	// defer i.lk.Unlock()

	// for _, p := range i.stores {
	// 	_, ok := p.bindSectors[sector]
	// 	if ok {
	// 		delete(p.bindSectors, sector)
	// 		log.Debugf("UnBindSector2SealStorage ok: sector %s, storage ID:%s", sector, p.info.ID)
	// 		return nil
	// 	}
	// }

	// log.Debugf("UnBindSector2SealStorage ok: sector %s not yet bind to any storage", sector)
	return nil
}

func (i *Index) StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	byID := map[storiface.ID]map[abi.SectorID]storiface.SectorFileType{}

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

	out := map[storiface.ID][]storiface.Decl{}
	for id, m := range byID {
		out[id] = []storiface.Decl{}
		for sectorID, fileType := range m {
			out[id] = append(out[id], storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: fileType,
			})
		}
	}

	return out, nil
}

func (i *Index) StorageAttach(ctx context.Context, si storiface.StorageInfo, st fsutil.FsStat) error {
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

		var s = i.stores[si.ID]
		s.info.URLs = si.URLs
		s.info.Weight = si.Weight
		s.info.MaxStorage = si.MaxStorage
		s.info.CanSeal = si.CanSeal
		s.info.CanStore = si.CanStore
		s.info.GroupID = si.GroupID
		s.info.MaxSealingSectors = si.MaxSealingSectors
		// clear bind sectors
		s.bindSectors = make(map[abi.SectorID]struct{})

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

func (i *Index) StorageReportHealth(ctx context.Context, id storiface.ID, report storiface.HealthReport) error {
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

func (i *Index) StorageDeclareSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error {
	i.lk.Lock()
	defer i.lk.Unlock()

loop:
	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := storiface.Decl{SectorID: s, SectorFileType: fileType}
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
					log.Warnf("multiple sector %v storages, will prefer %s", s, storageID)
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
			log.Warnf("multiple sector %v storages, will prefer %s", s, storageID)
		}

		i.sectors[d] = arr2
	}

	store, exist := i.stores[storageID]
	if exist {
		store.bindSectors[s] = struct{}{}
	}

	return nil
}

func (i *Index) StorageDropSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := storiface.Decl{SectorID: s, SectorFileType: fileType}

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
	if exist {
		delete(store.bindSectors, s)
	}

	return nil
}

func (i *Index) StorageFindSector(ctx context.Context, s abi.SectorID, ft storiface.SectorFileType, ssize abi.SectorSize, allowFetch bool) ([]storiface.SectorStorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	storageIDs := map[storiface.ID]uint64{}
	isprimary := map[storiface.ID]bool{}

	//allowTo := map[storiface.Group]struct{}{}
	storageArray := make([]storiface.ID, 0, 4)

	for _, pathType := range storiface.PathTypes {
		if ft&pathType == 0 {
			continue
		}

		storages, exist := i.sectors[storiface.Decl{s, pathType}]
		if exist {
			//log.Infof("StorageFindSector, sector %v, pathType:%d, storage:%v", s, pathType, storages)

			for _, id := range storages {
				_, exist := storageIDs[id.storage]
				if !exist {
					storageArray = append(storageArray, id.storage)
				}

				storageIDs[id.storage]++
				isprimary[id.storage] = isprimary[id.storage] || id.primary
			}
		}
	}

	out := make([]storiface.SectorStorageInfo, 0, len(storageIDs))

	for _, id := range storageArray {
		st, ok := i.stores[id]
		if !ok {
			log.Warnf("storage %s is not present in sector index (referenced by sector %v)", id, s)
			continue
		}

		urls, burls := make([]string, len(st.info.URLs)), make([]string, len(st.info.URLs))
		for k, u := range st.info.URLs {
			rl, err := url.Parse(u)
			if err != nil {
				log.Errorf("StorageFindSector failed to parse url: %w", err)
				continue
			}

			rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
			urls[k] = rl.String()
			burls[k] = u
		}

		if len(urls) < 1 {
			log.Errorf("StorageFindSector find sector %v failed, storage id:%s has no url to access", s, id)
			continue
		}

		n := storageIDs[id]
		out = append(out, storiface.SectorStorageInfo{
			GroupID:           st.info.GroupID,
			MaxSealingSectors: st.info.MaxSealingSectors,
			ID:                id,
			URLs:              urls,
			BaseURLs:          burls,
			Weight:            st.info.Weight * n, // storage with more sector types is better

			CanSeal:  st.info.CanSeal,
			CanStore: st.info.CanStore,

			Primary: isprimary[id],
		})

		//log.Infof("StorageFindSector, sector %v, out:%s", s, id)
	}

	//if allowFetch {
	//	spaceReq, err := ft.SealSpaceUse(ssize)
	//	if err != nil {
	//		return nil, xerrors.Errorf("estimating required space: %w", err)
	//	}

	//	for id, st := range i.stores {
	//		if !st.info.CanSeal {
	//			continue
	//		}

	//		if spaceReq > uint64(st.fsi.Available) {
	//			log.Debugf("not selecting on %s, out of space (available: %d, need: %d)", st.info.ID, st.fsi.Available, spaceReq)
	//			continue
	//		}
	//
	//		if time.Since(st.lastHeartbeat) > SkippedHeartbeatThresh {
	//			log.Debugf("not selecting on %s, didn't receive heartbeats for %s", st.info.ID, time.Since(st.lastHeartbeat))
	//			continue
	//		}
	//
	//		if st.heartbeatErr != nil {
	//			log.Debugf("not selecting on %s, heartbeat error: %s", st.info.ID, st.heartbeatErr)
	//			continue
	//		}
	//
	//		if _, ok := storageIDs[id]; ok {
	//			continue
	//		}
	//
	//		if allowTo != nil {
	//			allow := false
	//			for _, group := range st.info.Groups {
	//				if _, found := allowTo[group]; found {
	//					log.Debugf("path %s in allowed group %s", st.info.ID, group)
	//					allow = true
	//					break
	//				}
	//			}
	//
	//			if !allow {
	//				log.Debugf("not selecting on %s, not in allowed group, allow %+v; path has %+v", st.info.ID, allowTo, st.info.Groups)
	//				continue
	//			}
	//		}
	//
	//		urls, burls := make([]string, len(st.info.URLs)), make([]string, len(st.info.URLs))
	//		for k, u := range st.info.URLs {
	//			rl, err := url.Parse(u)
	//			if err != nil {
	//				return nil, xerrors.Errorf("failed to parse url: %w", err)
	//			}
	//
	//			rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
	//			urls[k] = rl.String()
	//			burls[k] = u
	//		}
	//
	//		out = append(out, storiface.SectorStorageInfo{
	//			ID:       id,
	//			URLs:     urls,
	//			BaseURLs: burls,
	//			Weight:   st.info.Weight * 0, // TODO: something better than just '0'
	//
	//			CanSeal:  st.info.CanSeal,
	//			CanStore: st.info.CanStore,
	//
	//			Primary: false,
	//		})
	//	}
	//}

	if ssize > 0 {
		i.sectorSizeMemo = uint64(ssize)
	}

	return out, nil
}

func (i *Index) StorageInfo(ctx context.Context, id storiface.ID) (storiface.StorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	si, found := i.stores[id]
	if !found {
		return storiface.StorageInfo{}, xerrors.Errorf("sector store not found")
	}

	return *si.info, nil
}

func (i *Index) StorageBestAlloc(ctx context.Context, allocate storiface.SectorFileType, ssize abi.SectorSize, pathType storiface.PathType) ([]storiface.StorageInfo, error) {
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

	if ssize > 0 {
		i.sectorSizeMemo = uint64(ssize)
	}

	for _, p := range i.stores {
		if (pathType == storiface.PathSealing) && !p.info.CanSeal {
			continue
		}
		if (pathType == storiface.PathStorage) && !p.info.CanStore {
			continue
		}

		if p.info.MaxSealingSectors > 0 && len(p.bindSectors) >= p.info.MaxSealingSectors {
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
		log.Debugf("StorageBestAlloc, no good path found")
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

	out := make([]storiface.StorageInfo, len(candidates))
	for i, candidate := range candidates {
		out[i] = *candidate.info
	}

	return out, nil
}

func (i *Index) FindSector(id abi.SectorID, typ storiface.SectorFileType) ([]storiface.ID, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	f, ok := i.sectors[storiface.Decl{
		SectorID:       id,
		SectorFileType: typ,
	}]
	if !ok {
		return nil, nil
	}
	out := make([]storiface.ID, 0, len(f))
	for _, meta := range f {
		out = append(out, meta.storage)
	}

	return out, nil
}

var _ SectorIndex = &Index{}
