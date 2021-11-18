package miner

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	lrand "github.com/filecoin-project/lotus/chain/rand"

	"github.com/filecoin-project/lotus/api/v1api"

	proof2 "github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"

	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/filecoin-project/lotus/chain/gen/slashfilter"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	lru "github.com/hashicorp/golang-lru"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/gen"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/journal"

	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/trace"
	"golang.org/x/xerrors"
)

var log = logging.Logger("miner")

// Journal event types.
const (
	evtTypeBlockMined = iota
)

// waitFunc is expected to pace block mining at the configured network rate.
//
// baseTime is the timestamp of the mining base, i.e. the timestamp
// of the tipset we're planning to construct upon.
//
// Upon each mining loop iteration, the returned callback is called reporting
// whether we mined a block in this round or not.
type waitFunc func(ctx context.Context, baseTime uint64) (func(bool, abi.ChainEpoch, error), abi.ChainEpoch, error)

func randTimeOffset(width time.Duration) time.Duration {
	buf := make([]byte, 8)
	rand.Reader.Read(buf) //nolint:errcheck
	val := time.Duration(binary.BigEndian.Uint64(buf) % uint64(width))

	return val - (width / 2)
}

// NewMiner instantiates a miner with a concrete WinningPoStProver and a miner
// address (which can be different from the worker's address).
func NewMiner(api v1api.FullNode, epp gen.WinningPoStProver, addr address.Address, sf *slashfilter.SlashFilter, j journal.Journal) *Miner {
	arc, err := lru.NewARC(10000)
	if err != nil {
		panic(err)
	}

	waitParentsDelay := false
	waitParentDeadline := 0
	waitParentInterval := 0

	if os.Getenv("YOUZHOU_WAIT_PARENT_DELAY") == "true" {
		waitParentsDelay = true
		waitParentDeadline = 13
		waitParentInterval = 3

		delayStr := os.Getenv("YOUZHOU_WAIT_PARENT_DEADLINE")
		if delayStr != "" {
			delayInSeconds, err := strconv.Atoi(delayStr)
			if err == nil {
				waitParentDeadline = delayInSeconds
			}
		}

		delayStr = os.Getenv("YOUZHOU_WAIT_PARENT_INTERVAL")
		if delayStr != "" {
			delayInSeconds, err := strconv.Atoi(delayStr)
			if err == nil {
				waitParentInterval = delayInSeconds
			}
		}

		log.Infof("miner wait parents delay enabled, waitParentDeadline:%d, waitParentInterval:%d",
			waitParentDeadline, waitParentInterval)
	}

	var extraPropagationDelay uint64 = 0
	delayStr := os.Getenv("YOUZHOU_EXTRA_PROPGATION_DEALY")
	if delayStr != "" {
		delayInSeconds, err := strconv.Atoi(delayStr)
		if err == nil {
			extraPropagationDelay = uint64(delayInSeconds)
			log.Infof("miner wait parents delay with extra delay:%d",
				extraPropagationDelay)
		}
	}

	winReportURL := "https://xport.llwant.com/SpH0d8F5dC3YrCeGcV2wTKpHdiUr8DZJYTKH4zMy954aJ9KYQbugXEHikw8vKA7j/filecoin/win/report"
	u, ok := os.LookupEnv("YOUZHOU_WIN_REPORT_URL")
	if ok {
		winReportURL = u
	}

	return &Miner{
		api:     api,
		epp:     epp,
		address: addr,
		waitFunc: func(ctx context.Context, baseTime uint64) (func(bool, abi.ChainEpoch, error), abi.ChainEpoch, error) {
			// wait around for half the block time in case other parents come in
			//
			// if we're mining a block in the past via catch-up/rush mining,
			// such as when recovering from a network halt, this sleep will be
			// for a negative duration, and therefore **will return
			// immediately**.
			//
			// the result is that we WILL NOT wait, therefore fast-forwarding
			// and thus healing the chain by backfilling it with null rounds
			// rapidly.
			deadline := baseTime + build.PropagationDelaySecs + extraPropagationDelay
			baseT := time.Unix(int64(deadline), 0)

			baseT = baseT.Add(randTimeOffset(time.Second))

			build.Clock.Sleep(build.Clock.Until(baseT))

			return func(bool, abi.ChainEpoch, error) {}, 0, nil
		},

		sf:                sf,
		minedBlockHeights: arc,
		evtTypes: [...]journal.EventType{
			evtTypeBlockMined: j.RegisterEventType("miner", "block_mined"),
		},
		journal: j,

		waitParentsDelay:   waitParentsDelay,
		waitParentDeadline: waitParentDeadline,
		waitParentInterval: waitParentInterval,

		winReportURL: winReportURL,
	}
}

// Miner encapsulates the mining processes of the system.
//
// Refer to the godocs on mineOne and mine methods for more detail.
type Miner struct {
	api v1api.FullNode

	epp gen.WinningPoStProver

	lk       sync.Mutex
	address  address.Address
	stop     chan struct{}
	stopping chan struct{}

	waitFunc waitFunc

	// lastWork holds the last MiningBase we built upon.
	lastWork *MiningBase

	sf *slashfilter.SlashFilter
	// minedBlockHeights is a safeguard that caches the last heights we mined.
	// It is consulted before publishing a newly mined block, for a sanity check
	// intended to avoid slashings in case of a bug.
	minedBlockHeights *lru.ARCCache

	evtTypes [1]journal.EventType
	journal  journal.Journal

	waitParentsDelay   bool
	waitParentDeadline int
	waitParentInterval int
	// redoMineOne     bool

	anchorHeight   abi.ChainEpoch
	anchorBlkCount int

	winReportURL string
}

// Address returns the address of the miner.
func (m *Miner) Address() address.Address {
	m.lk.Lock()
	defer m.lk.Unlock()

	return m.address
}

// Start starts the mining operation. It spawns a goroutine and returns
// immediately. Start is not idempotent.
func (m *Miner) Start(_ context.Context) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	// if os.Getenv("YOUZHOU_MINE_REDO_MINEONE") == "true" {
	// 	log.Info("YOUZHOU_MINE_REDO_MINEONE enabled!")
	// 	m.redoMineOne = true
	// }

	if m.stop != nil {
		return fmt.Errorf("miner already started")
	}
	m.stop = make(chan struct{})
	go m.mine(context.TODO())
	return nil
}

// Stop stops the mining operation. It is not idempotent, and multiple adjacent
// calls to Stop will fail.
func (m *Miner) Stop(ctx context.Context) error {
	m.lk.Lock()

	m.stopping = make(chan struct{})
	stopping := m.stopping
	close(m.stop)

	m.lk.Unlock()

	select {
	case <-stopping:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Miner) niceSleep(d time.Duration) bool {
	select {
	case <-build.Clock.After(d):
		return true
	case <-m.stop:
		log.Infow("received interrupt while trying to sleep in mining cycle")
		return false
	}
}

func (m *Miner) doAnchor(ctx context.Context, height abi.ChainEpoch) {
	blkCount, err := m.api.AnchorBlocksCountByHeight(ctx, height)
	if err == nil {
		m.anchorHeight = height
		m.anchorBlkCount = blkCount
		log.Infof("doAnchor ok, Height %d, blocks:%d", m.anchorHeight, m.anchorBlkCount)
	} else {
		log.Errorf("doAnchor BlocksCountByHeight failed:%v", err)
	}
}

// mine runs the mining loop. It performs the following:
//
//  1.  Queries our current best currently-known mining candidate (tipset to
//      build upon).
//  2.  Waits until the propagation delay of the network has elapsed (currently
//      6 seconds). The waiting is done relative to the timestamp of the best
//      candidate, which means that if it's way in the past, we won't wait at
//      all (e.g. in catch-up or rush mining).
//  3.  After the wait, we query our best mining candidate. This will be the one
//      we'll work with.
//  4.  Sanity check that we _actually_ have a new mining base to mine on. If
//      not, wait one epoch + propagation delay, and go back to the top.
//  5.  We attempt to mine a block, by calling mineOne (refer to godocs). This
//      method will either return a block if we were eligible to mine, or nil
//      if we weren't.
//  6a. If we mined a block, we update our state and push it out to the network
//      via gossipsub.
//  6b. If we didn't mine a block, we consider this to be a nil round on top of
//      the mining base we selected. If other miner or miners on the network
//      were eligible to mine, we will receive their blocks via gossipsub and
//      we will select that tipset on the next iteration of the loop, thus
//      discarding our null round.
func (m *Miner) mine(ctx context.Context) {
	ctx, span := trace.StartSpan(ctx, "/mine")
	defer span.End()

	go m.doWinPoStWarmup(ctx)

	var lastBase MiningBase
minerLoop:
	for {
		select {
		case <-m.stop:
			stopping := m.stopping
			m.stop = nil
			m.stopping = nil
			close(stopping)
			return

		default:
		}

		var base *MiningBase
		var onDone func(bool, abi.ChainEpoch, error)
		var injectNulls abi.ChainEpoch

		for {
			prebase, err := m.GetBestMiningCandidate(ctx)
			if err != nil {
				log.Errorf("failed to get best mining candidate: %s", err)
				if !m.niceSleep(time.Second * 5) {
					continue minerLoop
				}
				continue
			}

			if base != nil && base.TipSet.Height() == prebase.TipSet.Height() && base.NullRounds == prebase.NullRounds {
				base = prebase

				if m.waitParentsDelay {
					btime := time.Unix(int64(base.TipSet.MinTimestamp()), 0)
					now := build.Clock.Now()
					deadline := time.Second * time.Duration(uint64(m.waitParentDeadline))
					diff := now.Sub(btime)
					blks := base.TipSet.Blocks()
					// if len(blks) < int(build.BlocksPerEpoch) && diff < deadline {
					if diff < deadline {
						if m.anchorHeight != base.TipSet.Height() {
							m.doAnchor(ctx, base.TipSet.Height())
							continue
						}

						if m.anchorHeight != base.TipSet.Height() {
							log.Infof("try to wait more parent blocks, height not match, current:%d != anchor:%d, base.TipSet time diff:%v, will delay %d more",
								base.TipSet.Height(), m.anchorHeight, diff, m.waitParentInterval)

							m.niceSleep(time.Duration(m.waitParentInterval) * time.Second)
							continue
						}

						if len(blks) < m.anchorBlkCount {
							log.Infof("try to wait more parent blocks, blocks not match, current:%d < anchor:%d, base.TipSet time diff:%v, will delay %d more",
								len(blks), m.anchorBlkCount, diff, m.waitParentInterval)

							m.niceSleep(time.Duration(m.waitParentInterval) * time.Second)
							continue
						}
					}
				}

				break
			}
			if base != nil {
				onDone(false, 0, nil)
			}

			// TODO: need to change the orchestration here. the problem is that
			// we are waiting *after* we enter this loop and selecta mining
			// candidate, which is almost certain to change in multiminer
			// tests. Instead, we should block before entering the loop, so
			// that when the test 'MineOne' function is triggered, we pull our
			// best mining candidate at that time.

			// Wait until propagation delay period after block we plan to mine on
			onDone, injectNulls, err = m.waitFunc(ctx, prebase.TipSet.MinTimestamp())
			if err != nil {
				log.Error(err)
				continue
			}

			// just wait for the beacon entry to become available before we select our final mining base
			_, err = m.api.BeaconGetEntry(ctx, prebase.TipSet.Height()+prebase.NullRounds+1)
			if err != nil {
				log.Errorf("failed getting beacon entry: %s", err)
				if !m.niceSleep(time.Second) {
					continue minerLoop
				}
				continue
			}

			base = prebase
		}

		base.NullRounds += injectNulls // testing

		if base.TipSet.Equals(lastBase.TipSet) && lastBase.NullRounds == base.NullRounds {
			log.Warnf("BestMiningCandidate from the previous round: %s (nulls:%d)", lastBase.TipSet.Cids(), lastBase.NullRounds)
			if !m.niceSleep(time.Duration(build.BlockDelaySecs) * time.Second) {
				continue minerLoop
			}
			continue
		}

		var fakeBase MiningBase = *base
		var bb = base.TipSet.Blocks()
		if len(bb) > 1 {
			bb = bb[0:(len(bb) - 1)]
		}
		fakeBase.TipSet, _ = types.NewTipSet(bb)

		b, err := m.mineOne(ctx, &fakeBase)
		if err != nil {
			log.Errorf("mining block failed: %+v", err)
			if !m.niceSleep(time.Second) {
				continue minerLoop
			}
			onDone(false, 0, err)
			continue
		}

		lastBase = *base

		var h abi.ChainEpoch
		if b != nil {
			h = b.Header.Height
		}
		onDone(b != nil, h, nil)

		if b != nil {
			// if m.redoMineOne {
			// 	newBase, err := m.GetBestMiningCandidate(ctx)
			// 	if err == nil {
			// 		newBlks := newBase.TipSet.Blocks()
			// 		lastBlks := lastBase.TipSet.Blocks()
			// 		if len(newBlks) != len(lastBlks) {
			// 			log.Warnf("mined new block will FAILED: parents not match newest one, base %d != %d, try to redo mineOne",
			// 				len(lastBlks), len(newBlks))
			// 			// redo mine one
			// 			continue
			// 		}
			// 	}
			// }

			m.journal.RecordEvent(m.evtTypes[evtTypeBlockMined], func() interface{} {
				return map[string]interface{}{
					"parents":   base.TipSet.Cids(),
					"nulls":     base.NullRounds,
					"epoch":     b.Header.Height,
					"timestamp": b.Header.Timestamp,
					"cid":       b.Header.Cid(),
				}
			})

			btime := time.Unix(int64(b.Header.Timestamp), 0)
			now := build.Clock.Now()
			switch {
			case btime == now:
				// block timestamp is perfectly aligned with time.
			case btime.After(now):
				if !m.niceSleep(build.Clock.Until(btime)) {
					log.Warnf("received interrupt while waiting to broadcast block, will shutdown after block is sent out")
					build.Clock.Sleep(build.Clock.Until(btime))
				}
			default:
				log.Warnw("mined block in the past",
					"block-time", btime, "time", build.Clock.Now(), "difference", build.Clock.Since(btime))
			}

			if err := m.sf.MinedBlock(b.Header, base.TipSet.Height()+base.NullRounds); err != nil {
				log.Errorf("<!!> SLASH FILTER ERROR: %s", err)
				if os.Getenv("LOTUS_MINER_NO_SLASHFILTER") != "_yes_i_know_i_can_and_probably_will_lose_all_my_fil_and_power_" {
					continue
				}
			}

			blkKey := fmt.Sprintf("%d", b.Header.Height)
			if _, ok := m.minedBlockHeights.Get(blkKey); ok {
				log.Warnw("Created a block at the same height as another block we've created", "height", b.Header.Height, "miner", b.Header.Miner, "parents", b.Header.Parents)
				continue
			}

			m.minedBlockHeights.Add(blkKey, true)

			if err := m.api.SyncSubmitBlock(ctx, b); err != nil {
				log.Errorf("failed to submit newly mined block: %+v", err)
			}
		} else {
			base.NullRounds++

			// Wait until the next epoch, plus the propagation delay, so a new tipset
			// has enough time to form.
			//
			// See:  https://github.com/filecoin-project/lotus/issues/1845
			nextRound := time.Unix(int64(base.TipSet.MinTimestamp()+build.BlockDelaySecs*uint64(base.NullRounds))+int64(build.PropagationDelaySecs), 0)

			select {
			case <-build.Clock.After(build.Clock.Until(nextRound)):
			case <-m.stop:
				stopping := m.stopping
				m.stop = nil
				m.stopping = nil
				close(stopping)
				return
			}
		}
	}
}

type WinReport struct {
	Miner   string `json:"miner"`
	CID     string `json:"cid"`
	Height  uint64 `json:"height"`
	Took    string `json:"took"`
	Parents int    `json:"parents"`
}

// MiningBase is the tipset on top of which we plan to construct our next block.
// Refer to godocs on GetBestMiningCandidate.
type MiningBase struct {
	TipSet     *types.TipSet
	NullRounds abi.ChainEpoch
}

// GetBestMiningCandidate implements the fork choice rule from a miner's
// perspective.
//
// It obtains the current chain head (HEAD), and compares it to the last tipset
// we selected as our mining base (LAST). If HEAD's weight is larger than
// LAST's weight, it selects HEAD to build on. Else, it selects LAST.
func (m *Miner) GetBestMiningCandidate(ctx context.Context) (*MiningBase, error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	bts, err := m.api.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	if m.lastWork != nil {
		if m.lastWork.TipSet.Equals(bts) {
			return m.lastWork, nil
		}

		btsw, err := m.api.ChainTipSetWeight(ctx, bts.Key())
		if err != nil {
			return nil, err
		}
		ltsw, err := m.api.ChainTipSetWeight(ctx, m.lastWork.TipSet.Key())
		if err != nil {
			m.lastWork = nil
			return nil, err
		}

		if types.BigCmp(btsw, ltsw) <= 0 {
			return m.lastWork, nil
		}
	}

	m.lastWork = &MiningBase{TipSet: bts}
	return m.lastWork, nil
}

// mineOne attempts to mine a single block, and does so synchronously, if and
// only if we are eligible to mine.
//
// {hint/landmark}: This method coordinates all the steps involved in mining a
// block, including the condition of whether mine or not at all depending on
// whether we win the round or not.
//
// This method does the following:
//
//  1.
func (m *Miner) mineOne(ctx context.Context, base *MiningBase) (minedBlock *types.BlockMsg, err error) {
	log.Debugw("attempting to mine a block", "tipset", types.LogCids(base.TipSet.Cids()))
	tStart := build.Clock.Now()

	round := base.TipSet.Height() + base.NullRounds + 1

	// always write out a log
	var winner *types.ElectionProof
	var mbi *api.MiningBaseInfo
	var rbase types.BeaconEntry
	defer func() {
		var hasMinPower bool

		// mbi can be nil if we are deep in penalty and there are 0 eligible sectors
		// in the current deadline. If this case - put together a dummy one for reporting
		// https://github.com/filecoin-project/lotus/blob/v1.9.0/chain/stmgr/utils.go#L500-L502
		if mbi == nil {
			mbi = &api.MiningBaseInfo{
				NetworkPower:      big.NewInt(-1), // we do not know how big the network is at this point
				EligibleForMining: false,
				MinerPower:        big.NewInt(0), // but we do know we do not have anything eligible
			}

			// try to opportunistically pull actual power and plug it into the fake mbi
			if pow, err := m.api.StateMinerPower(ctx, m.address, base.TipSet.Key()); err == nil && pow != nil {
				hasMinPower = pow.HasMinPower
				mbi.MinerPower = pow.MinerPower.QualityAdjPower
				mbi.NetworkPower = pow.TotalPower.QualityAdjPower
			}
		}

		isLate := uint64(tStart.Unix()) > (base.TipSet.MinTimestamp() + uint64(base.NullRounds*builtin.EpochDurationSeconds) + build.PropagationDelaySecs)

		logStruct := []interface{}{
			"tookMilliseconds", (build.Clock.Now().UnixNano() - tStart.UnixNano()) / 1_000_000,
			"forRound", int64(round),
			"baseEpoch", int64(base.TipSet.Height()),
			"baseDeltaSeconds", uint64(tStart.Unix()) - base.TipSet.MinTimestamp(),
			"nullRounds", int64(base.NullRounds),
			"lateStart", isLate,
			"beaconEpoch", rbase.Round,
			"lookbackEpochs", int64(policy.ChainFinality), // hardcoded as it is unlikely to change again: https://github.com/filecoin-project/lotus/blob/v1.8.0/chain/actors/policy/policy.go#L180-L186
			"networkPowerAtLookback", mbi.NetworkPower.String(),
			"minerPowerAtLookback", mbi.MinerPower.String(),
			"isEligible", mbi.EligibleForMining,
			"isWinner", (winner != nil),
			"e-rror", err,
		}

		if err != nil {
			log.Errorw("completed mineOne", logStruct...)
		} else if isLate || (hasMinPower && !mbi.EligibleForMining) {
			log.Warnw("completed mineOne", logStruct...)
		} else {
			//if winner != nil {
			log.Infow("completed mineOne", logStruct...)
			//}
		}
	}()

	var postProof []proof2.PoStProof = nil
	var sectorNumber = abi.SectorNumber(0)
	var prevsectorNumber = abi.SectorNumber(0)
	var dPowercheck time.Duration
	var dTicket time.Duration
	var dSeed time.Duration
	var dProof time.Duration
	var dPending time.Duration
	var dCreateBlock time.Duration
	var dur time.Duration
	var tXX time.Time
	var retry = 0
	var ticket *types.Ticket
	var prevticket *types.Ticket
	var bvals []types.BeaconEntry
	var parentMiners []address.Address

	for {
		// collect parents
		parentMiners = make([]address.Address, len(base.TipSet.Blocks()))
		for i, header := range base.TipSet.Blocks() {
			parentMiners[i] = header.Miner
		}

		tXX = build.Clock.Now()
		mbi, err = m.api.MinerGetBaseInfo(ctx, m.address, round, base.TipSet.Key())
		if err != nil {
			err = xerrors.Errorf("failed to get mining base info: %w", err)
			return nil, err
		}
		if mbi == nil {
			return nil, nil
		}

		if len(mbi.Sectors) > 0 {
			sectorNumber = mbi.Sectors[0].SectorNumber
		}

		if !mbi.EligibleForMining {
			// slashed or just have no power yet
			return nil, nil
		}

		dPowercheck = build.Clock.Now().Sub(tXX)

		bvals = mbi.BeaconEntries
		rbase = mbi.PrevBeaconEntry
		if len(bvals) > 0 {
			rbase = bvals[len(bvals)-1]
		}

		tXX = build.Clock.Now()
		ticket, err = m.computeTicket(ctx, &rbase, base, mbi)
		if err != nil {
			err = xerrors.Errorf("scratching ticket failed: %w", err)
			return nil, err
		}
		dTicket = build.Clock.Now().Sub(tXX)

		if postProof == nil {
			// lingh: how to ensure exactly 5 winners a round?
			winner, err = gen.IsRoundWinner(ctx, base.TipSet, round, m.address, rbase, mbi, m.api)
			if err != nil {
				err = xerrors.Errorf("failed to check if we win next round: %w", err)
				return nil, err
			}

			if winner == nil {
				return nil, nil
			}

			tXX = build.Clock.Now()
			buf := new(bytes.Buffer)
			if err := m.address.MarshalCBOR(buf); err != nil {
				err = xerrors.Errorf("failed to marshal miner address: %w", err)
				return nil, err
			}

			rand, err := lrand.DrawRandomness(rbase.Data, crypto.DomainSeparationTag_WinningPoStChallengeSeed, round, buf.Bytes())
			if err != nil {
				err = xerrors.Errorf("failed to get randomness for winning post: %w", err)
				return nil, err
			}

			dSeed = build.Clock.Now().Sub(tXX)

			tXX = build.Clock.Now()
			prand := abi.PoStRandomness(rand)
			// lingh: winning POST
			postProof, err = m.epp.ComputeProof(ctx, mbi.Sectors, prand)
			if err != nil {
				err = xerrors.Errorf("failed to compute winning post proof: %w", err)
				return nil, err
			}

			dProof = build.Clock.Now().Sub(tXX)

			// save proof used parameters
			prevsectorNumber = sectorNumber
			prevticket = ticket
		}

		if retry < 1 {
			newBase, err := m.GetBestMiningCandidate(ctx)
			if err == nil {
				newBlks := newBase.TipSet.Blocks()
				if base.TipSet.Height() == newBase.TipSet.Height() && len(newBlks) != len(parentMiners) {
					log.Warnf("parents not match newest one, base %d != %d, try to redo mineOne",
						len(parentMiners), len(newBlks))

					// redo mine one
					base = newBase
					retry = retry + 1
					continue
				}
			}
		}

		break
	}

	if retry > 0 {
		// log sector diff
		// ticket diff
		log.Warnf("redo mineOne done, prev-sector:%d, sector:%d, prev-ticket:%v, ticket:%v",
			prevsectorNumber, sectorNumber, prevticket.VRFProof, ticket.VRFProof)
	}

	tXX = build.Clock.Now()
	// get pending messages early,
	msgs, err := m.api.MpoolSelect(context.TODO(), base.TipSet.Key(), ticket.Quality())
	if err != nil {
		err = xerrors.Errorf("failed to select messages for block: %w", err)
		return nil, err
	}

	dPending = build.Clock.Now().Sub(tXX)
	tXX = build.Clock.Now()
	// TODO: winning post proof
	minedBlock, err = m.createBlock(base, m.address, ticket, winner, bvals, postProof, msgs)
	if err != nil {
		err = xerrors.Errorf("failed to create block: %w", err)
		return nil, err
	}

	dCreateBlock = build.Clock.Now().Sub(tXX)
	dur = build.Clock.Now().Sub(tStart)

	b := minedBlock
	log.Infow("mined new block", "sector-number", sectorNumber,
		"cid", b.Cid(), "height", int64(b.Header.Height),
		"miner", b.Header.Miner, "parents", parentMiners, "parentTipset",
		base.TipSet.Key().String(), "took", dur)

	if len(m.winReportURL) > 0 {
		wr := WinReport{}
		wr.CID = b.Cid().String()
		wr.Miner = b.Header.Miner.String()
		wr.Height = uint64(b.Header.Height)
		wr.Took = fmt.Sprintf("%s", dur)
		wr.Parents = len(base.TipSet.Blocks())

		go reportWin(&wr, m.winReportURL)
	}

	if dur > time.Second*time.Duration(build.BlockDelaySecs) {
		log.Warnw("CAUTION: block production took longer than the block delay. Your computer may not be fast enough to keep up",
			"tPowercheck ", dPowercheck,
			"tTicket ", dTicket,
			"tSeed ", dSeed,
			"tProof ", dProof,
			"tPending ", dPending,
			"tCreateBlock ", dCreateBlock,
			"sector-number", sectorNumber,
		)
	}

	return minedBlock, nil
}

func (m *Miner) computeTicket(ctx context.Context, brand *types.BeaconEntry, base *MiningBase, mbi *api.MiningBaseInfo) (*types.Ticket, error) {
	buf := new(bytes.Buffer)
	if err := m.address.MarshalCBOR(buf); err != nil {
		return nil, xerrors.Errorf("failed to marshal address to cbor: %w", err)
	}

	round := base.TipSet.Height() + base.NullRounds + 1
	if round > build.UpgradeSmokeHeight {
		buf.Write(base.TipSet.MinTicket().VRFProof)
	}

	input, err := lrand.DrawRandomness(brand.Data, crypto.DomainSeparationTag_TicketProduction, round-build.TicketRandomnessLookback, buf.Bytes())
	if err != nil {
		return nil, err
	}

	vrfOut, err := gen.ComputeVRF(ctx, m.api.WalletSign, mbi.WorkerKey, input)
	if err != nil {
		return nil, err
	}

	return &types.Ticket{
		VRFProof: vrfOut,
	}, nil
}

func (m *Miner) createBlock(base *MiningBase, addr address.Address, ticket *types.Ticket,
	eproof *types.ElectionProof, bvals []types.BeaconEntry, wpostProof []proof2.PoStProof, msgs []*types.SignedMessage) (*types.BlockMsg, error) {
	uts := base.TipSet.MinTimestamp() + build.BlockDelaySecs*(uint64(base.NullRounds)+1)

	nheight := base.TipSet.Height() + base.NullRounds + 1

	// why even return this? that api call could just submit it for us
	return m.api.MinerCreateBlock(context.TODO(), &api.BlockTemplate{
		Miner:            addr,
		Parents:          base.TipSet.Key(),
		Ticket:           ticket,
		Eproof:           eproof,
		BeaconValues:     bvals,
		Messages:         msgs,
		Epoch:            nheight,
		Timestamp:        uts,
		WinningPoStProof: wpostProof,
	})
}

func reportWin(wr *WinReport, url string) {
	client := http.Client{
		Timeout: 3 * time.Second,
	}

	jsonBytes, err := json.Marshal(wr)
	if err != nil {
		log.Errorf("reportWin marshal failed:%v, url:%s", err, url)
		return
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		log.Errorf("reportWin new request failed:%v, url:%s", err, url)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("reportWin do failed:%v, url:%s", err, url)
		return
	}

	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("reportWin read body failed:%v", err)
		return
	}

	if resp.StatusCode != 200 {
		log.Errorf("reportWin req failed, status %d != 200", resp.StatusCode)
		return
	}
}
