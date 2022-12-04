package sealer

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"

	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/proof"

	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

var PostCheckTimeout = 160 * time.Second

// FaultTracker TODO: Track things more actively
type FaultTracker interface {
	CheckProvable(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storiface.SectorRef, rg storiface.RGetter) (map[abi.SectorID]string, error)
}

// CheckProvable returns unprovable sectors
func (m *Manager) CheckProvableOffice(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storiface.SectorRef, rg storiface.RGetter) (map[abi.SectorID]string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if rg == nil {
		return nil, xerrors.Errorf("rg is nil")
	}

	var bad = make(map[abi.SectorID]string)
	var badLk sync.Mutex

	var postRand abi.PoStRandomness = make([]byte, abi.RandomnessLength)
	_, _ = rand.Read(postRand)
	postRand[31] &= 0x3f

	limit := m.parallelCheckLimit
	if limit <= 0 {
		limit = len(sectors)
	}
	throttle := make(chan struct{}, limit)

	addBad := func(s abi.SectorID, reason string) {
		badLk.Lock()
		bad[s] = reason
		badLk.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(len(sectors))

	for _, sector := range sectors {
		select {
		case throttle <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		go func(sector storiface.SectorRef) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			commr, update, err := rg(ctx, sector.ID)
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: getting commR", "sector", sector, "sealed", "err", err)
				addBad(sector.ID, fmt.Sprintf("getting commR: %s", err))
				return
			}

			toLock := storiface.FTSealed | storiface.FTCache
			if update {
				toLock = storiface.FTUpdate | storiface.FTUpdateCache
			}

			locked, err := m.index.StorageTryLock(ctx, sector.ID, toLock, storiface.FTNone)
			if err != nil {
				addBad(sector.ID, fmt.Sprintf("tryLock error: %s", err))
				return
			}

			if !locked {
				log.Warnw("CheckProvable Sector FAULT: can't acquire read lock", "sector", sector)
				addBad(sector.ID, fmt.Sprint("can't acquire read lock"))
				return
			}

			wpp, err := sector.ProofType.RegisteredWindowPoStProof()
			if err != nil {
				addBad(sector.ID, fmt.Sprint("can't get proof type"))
				return
			}

			ch, err := ffi.GeneratePoStFallbackSectorChallenges(wpp, sector.ID.Miner, postRand, []abi.SectorNumber{
				sector.ID.Number,
			})
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: generating challenges", "sector", sector, "err", err)
				addBad(sector.ID, fmt.Sprintf("generating fallback challenges: %s", err))
				return
			}

			vctx, cancel2 := context.WithTimeout(ctx, PostCheckTimeout)
			defer cancel2()

			_, err = m.storage.GenerateSingleVanillaProof(vctx, sector.ID.Miner, storiface.PostSectorChallenge{
				SealProof:    sector.ProofType,
				SectorNumber: sector.ID.Number,
				SealedCID:    commr,
				Challenge:    ch.Challenges[sector.ID.Number],
				Update:       update,
			}, wpp)
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: generating vanilla proof", "sector", sector, "err", err)
				addBad(sector.ID, fmt.Sprintf("generating vanilla proof: %s", err))
				return
			}
		}(sector)
	}

	wg.Wait()

	return bad, nil
}

// CheckProvable returns unprovable sectors
func (m *Manager) CheckProvable2(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storiface.SectorRef, rg storiface.RGetter) (map[abi.SectorID]string, error) {
	var bad = make(map[abi.SectorID]string)

	ssize, err := pp.SectorSize()
	if err != nil {
		return nil, err
	}

	var logLarge = os.Getenv("FIL_PROOFS_LOG_CHECK_SECTOR") == "true"
	var sizeExcludePaths = os.Getenv("FIL_PROOFS_SIZE_CHECK_EXCLUDE")
	var regx *regexp.Regexp = nil
	if sizeExcludePaths != "" {
		regx, err = regexp.Compile(sizeExcludePaths)
		if err != nil {
			log.Errorf("FIL_PROOFS_SIZE_CHECK_EXCLUDE not compile to valid regex: %s, %v", sizeExcludePaths, err)
		}
	}

	// TODO: More better checks
	for _, sector := range sectors {
		err := func() error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			var fReplica string
			var fCache string

			err := m.localStore.DiscoverSectorStore(ctx, sector.ID)
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: DiscoverSectorStore in checkProvable", "sector", sector, "error", err)
				bad[sector.ID] = fmt.Sprintf("acquire sector failed: %s", err)
				return nil
			}

			locked, err := m.index.StorageTryLock(ctx, sector.ID, storiface.FTSealed|storiface.FTCache, storiface.FTNone)
			if err != nil {
				return xerrors.Errorf("acquiring sector lock: %w", err)
			}

			if !locked {
				log.Warnw("CheckProvable Sector FAULT: can't acquire read lock", "sector", sector)
				bad[sector.ID] = fmt.Sprint("can't acquire read lock")
				return nil
			}

			lp, _, err := m.localStore.AcquireSector(ctx, sector, storiface.FTSealed|storiface.FTCache, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: acquire sector in checkProvable", "sector", sector, "error", err)
				bad[sector.ID] = fmt.Sprintf("acquire sector failed: %s", err)
				return nil
			}

			fReplica, fCache = lp.Sealed, lp.Cache

			if fReplica == "" || fCache == "" {
				log.Warnw("CheckProvable Sector FAULT: cache and/or sealed paths not found", "sector", sector, "sealed", fReplica, "cache", fCache)
				bad[sector.ID] = fmt.Sprintf("cache and/or sealed paths not found, cache %q, sealed %q", fCache, fReplica)
				return nil
			}

			var pAuxFileSize int64 = 0
			switch ssize {
			case 32 << 30:
				pAuxFileSize = 64
			case 64 << 30:
				pAuxFileSize = 64
			default:
			}

			toCheck := map[string]int64{
				fReplica:                       int64(ssize),
				filepath.Join(fCache, "p_aux"): pAuxFileSize,
			}

			skipCacheSizeCheck := false
			if regx != nil && regx.Match([]byte(lp.Cache)) {
				skipCacheSizeCheck = true
			}

			if !skipCacheSizeCheck {
				addCachePathsForSectorSize2(toCheck, lp.Cache, ssize)
			}

			start := time.Now()
			for p, sz := range toCheck {
				st, err := os.Stat(p)
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: sector file stat error", "sector", sector, "sealed", fReplica, "cache", fCache, "file", p, "err", err)
					bad[sector.ID] = fmt.Sprintf("%s", err)
					return nil
				}

				if sz != 0 {
					if st.Size() != sz {
						log.Warnw("CheckProvable Sector FAULT: sector file is wrong size", "sector", sector, "sealed", fReplica, "cache", fCache, "file", p, "size", st.Size(), "expectSize", sz)
						bad[sector.ID] = fmt.Sprintf("%s is wrong size (got %d, expect %d)", p, st.Size(), sz)
						return nil
					}
				}
			}

			if logLarge {
				var elapsed = time.Since(start)
				if elapsed >= time.Second {
					log.Warnw("CheckProvable Sector LARGE delay", "elapsed", elapsed, "sector", sector, "sealed", fReplica, "cache", fCache)
				}
			}

			if rg != nil {
				wpp, err := sector.ProofType.RegisteredWindowPoStProof()
				if err != nil {
					return err
				}

				var pr abi.PoStRandomness = make([]byte, abi.RandomnessLength)
				_, _ = rand.Read(pr)
				pr[31] &= 0x3f

				ch, err := ffi.GeneratePoStFallbackSectorChallenges(wpp, sector.ID.Miner, pr, []abi.SectorNumber{
					sector.ID.Number,
				})
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: generating challenges", "sector", sector, "sealed", fReplica, "cache", fCache, "err", err)
					bad[sector.ID] = fmt.Sprintf("generating fallback challenges: %s", err)
					return nil
				}

				commr, _, err := rg(ctx, sector.ID)
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: getting commR", "sector", sector, "sealed", fReplica, "cache", fCache, "err", err)
					bad[sector.ID] = fmt.Sprintf("getting commR: %s", err)
					return nil
				}

				_, err = ffi.GenerateSingleVanillaProof(ffi.PrivateSectorInfo{
					SectorInfo: proof.SectorInfo{
						SealProof:    sector.ProofType,
						SectorNumber: sector.ID.Number,
						SealedCID:    commr,
					},
					CacheDirPath:     fCache,
					PoStProofType:    wpp,
					SealedSectorPath: fReplica,
				}, ch.Challenges[sector.ID.Number])
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: generating vanilla proof", "sector", sector, "sealed", fReplica, "cache", fCache, "err", err)
					bad[sector.ID] = fmt.Sprintf("generating vanilla proof: %s", err)
					return nil
				}
			}

			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	return bad, nil
}

func (m *Manager) CheckProvable(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storiface.SectorRef, rg storiface.RGetter) (map[abi.SectorID]string, error) {
	result := make(map[abi.SectorID]string)
	if len(sectors) < 1 {
		return result, nil
	}

	var numCPU = runtime.NumCPU()
	var chunkSize = len(sectors) / numCPU
	if chunkSize < 1 {
		chunkSize = 1
	}

	var begin = 0
	var index = 0
	var chunkCnt = len(sectors)/chunkSize + 1
	var wg sync.WaitGroup
	var bads = make([]map[abi.SectorID]string, chunkCnt)
	var errs = make([]error, chunkCnt)

	for {
		var end = begin + chunkSize
		if end > len(sectors) {
			end = len(sectors)
		}

		var sectorsSplit = sectors[begin:end]
		var ix = index
		wg.Add(1)

		go func() {
			bad, err := m.CheckProvable2(ctx, pp, sectorsSplit, rg)
			bads[ix] = bad
			errs[ix] = err

			wg.Done()
		}()

		index = index + 1
		begin = end

		if begin >= len(sectors) {
			break
		}
	}

	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return result, e
		}
	}

	for _, m := range bads {
		for k, v := range m {
			result[k] = v
		}
	}

	return result, nil
}

func addCachePathsForSectorSize(chk map[string]int64, cacheDir string, ssize abi.SectorSize) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		chk[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = 0
	case 32 << 30:
		for i := 0; i < 8; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
}

func addCachePathsForSectorSize2(chk map[string]int64, cacheDir string, ssize abi.SectorSize) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		chk[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = 0
	case 32 << 30:
		for i := 0; i < 8; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 9586976
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 9586976
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
}

// CheckProvable returns unprovable sectors
func (m *Manager) CheckProvableExt(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storiface.SectorRef) (map[abi.SectorID]string, error) {
	var bad = make(map[abi.SectorID]string)

	ssize, err := pp.SectorSize()
	if err != nil {
		return nil, err
	}

	var logLarge = os.Getenv("FIL_PROOFS_LOG_CHECK_SECTOR") == "true"
	var sizeExcludePaths = os.Getenv("FIL_PROOFS_SIZE_CHECK_EXCLUDE")
	var regx *regexp.Regexp = nil
	if sizeExcludePaths != "" {
		regx, err = regexp.Compile(sizeExcludePaths)
		if err != nil {
			log.Errorf("FIL_PROOFS_SIZE_CHECK_EXCLUDE not compile to valid regex: %s, %v", sizeExcludePaths, err)
		}
	}

	// TODO: More better checks
	for _, sector := range sectors {
		err := func() error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			err := m.localStore.DiscoverSectorStore(ctx, sector.ID)
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: DiscoverSectorStore in checkProvable", "sector", sector, "error", err)
				bad[sector.ID] = fmt.Sprintf("acquire sector failed: %s", err)
				return nil
			}

			locked, err := m.index.StorageTryLock(ctx, sector.ID, storiface.FTSealed|storiface.FTCache, storiface.FTNone)
			if err != nil {
				return xerrors.Errorf("acquiring sector lock: %w", err)
			}

			if !locked {
				log.Warnw("CheckProvable Sector FAULT: can't acquire read lock", "sector", sector)
				bad[sector.ID] = fmt.Sprint("can't acquire read lock")
				return nil
			}

			lp, _, err := m.localStore.AcquireSector(ctx, sector, storiface.FTSealed|storiface.FTCache, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: acquire sector in checkProvable", "sector", sector, "error", err)
				bad[sector.ID] = fmt.Sprintf("acquire sector failed: %s", err)
				return nil
			}

			if lp.Sealed == "" || lp.Cache == "" {
				log.Warnw("CheckProvable Sector FAULT: cache and/or sealed paths not found", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache)
				bad[sector.ID] = fmt.Sprintf("cache and/or sealed paths not found, cache %q, sealed %q", lp.Cache, lp.Sealed)
				return nil
			}

			var pAuxFileSize int64 = 0
			switch ssize {
			case 32 << 30:
				pAuxFileSize = 64
			case 64 << 30:
				pAuxFileSize = 64
			default:
			}

			toCheck := map[string]int64{
				lp.Sealed:                        int64(ssize),
				filepath.Join(lp.Cache, "p_aux"): pAuxFileSize,
			}

			skipCacheSizeCheck := false
			if regx != nil && regx.Match([]byte(lp.Cache)) {
				skipCacheSizeCheck = true
			}

			if !skipCacheSizeCheck {
				addCachePathsForSectorSize2(toCheck, lp.Cache, ssize)
			}

			start := time.Now()
			for p, sz := range toCheck {
				st, err := os.Stat(p)
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: sector file stat error", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "file", p, "err", err)
					bad[sector.ID] = fmt.Sprintf("%s", err)
					return nil
				}

				if sz != 0 {
					if st.Size() != sz {
						log.Warnw("CheckProvable Sector FAULT: sector file is wrong size", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "file", p, "size", st.Size(), "expectSize", sz)
						bad[sector.ID] = fmt.Sprintf("%s is wrong size (got %d, expect %d)", p, st.Size(), sz)
						return nil
					}
				}
			}

			if logLarge {
				var elapsed = time.Since(start)
				if elapsed >= time.Second {
					log.Warnw("CheckProvable Sector LARGE delay", "elapsed", elapsed, "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache)
				}
			}

			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	return bad, nil
}

var _ FaultTracker = &Manager{}
