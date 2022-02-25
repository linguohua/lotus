package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/node/impl/client"
	"github.com/filecoin-project/lotus/node/impl/common"
	"github.com/filecoin-project/lotus/node/impl/full"
	"github.com/filecoin-project/lotus/node/impl/market"
	"github.com/filecoin-project/lotus/node/impl/net"
	"github.com/filecoin-project/lotus/node/impl/paych"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/lp2p"
)

var log = logging.Logger("node")
var AnchorData = AnchorData2{
	AnchorURLs:    []string{"https://xport.llwant.com/SpH0d8F5dC3YrCeGcV2wTKpHdiUr8DZJYTKH4zMy954aJ9KYQbugXEHikw8vKA7j/filecoin/head"},
	AnchorTimeout: 15,
}

type AnchorData2 struct {
	AnchorURLs    []string
	AnchorTimeout int
	HardcoreDelay int

	anchorLock        sync.Mutex
	anchorHeight      abi.ChainEpoch
	anchorQueryHeight abi.ChainEpoch
	anchorBlkCount    int
}

type FullNodeAPI struct {
	common.CommonAPI
	net.NetAPI
	full.ChainAPI
	client.API
	full.MpoolAPI
	full.GasAPI
	market.MarketAPI
	paych.PaychAPI
	full.StateAPI
	full.MsigAPI
	full.WalletAPI
	full.SyncAPI
	full.BeaconAPI

	DS          dtypes.MetadataDS
	NetworkName dtypes.NetworkName
}

func (n *FullNodeAPI) CreateBackup(ctx context.Context, fpath string) error {
	return backup(n.DS, fpath)
}

func (n *FullNodeAPI) NodeStatus(ctx context.Context, inclChainStatus bool) (status api.NodeStatus, err error) {
	curTs, err := n.ChainHead(ctx)
	if err != nil {
		return status, err
	}

	status.SyncStatus.Epoch = uint64(curTs.Height())
	timestamp := time.Unix(int64(curTs.MinTimestamp()), 0)
	delta := time.Since(timestamp).Seconds()
	status.SyncStatus.Behind = uint64(delta / 30)

	// get peers in the messages and blocks topics
	peersMsgs := make(map[peer.ID]struct{})
	peersBlocks := make(map[peer.ID]struct{})

	for _, p := range n.PubSub.ListPeers(build.MessagesTopic(n.NetworkName)) {
		peersMsgs[p] = struct{}{}
	}

	for _, p := range n.PubSub.ListPeers(build.BlocksTopic(n.NetworkName)) {
		peersBlocks[p] = struct{}{}
	}

	// get scores for all connected and recent peers
	scores, err := n.NetPubsubScores(ctx)
	if err != nil {
		return status, err
	}

	for _, score := range scores {
		if score.Score.Score > lp2p.PublishScoreThreshold {
			_, inMsgs := peersMsgs[score.ID]
			if inMsgs {
				status.PeerStatus.PeersToPublishMsgs++
			}

			_, inBlocks := peersBlocks[score.ID]
			if inBlocks {
				status.PeerStatus.PeersToPublishBlocks++
			}
		}
	}

	if inclChainStatus && status.SyncStatus.Epoch > uint64(build.Finality) {
		blockCnt := 0
		ts := curTs

		for i := 0; i < 100; i++ {
			blockCnt += len(ts.Blocks())
			tsk := ts.Parents()
			ts, err = n.ChainGetTipSet(ctx, tsk)
			if err != nil {
				return status, err
			}
		}

		status.ChainStatus.BlocksPerTipsetLast100 = float64(blockCnt) / 100

		for i := 100; i < int(build.Finality); i++ {
			blockCnt += len(ts.Blocks())
			tsk := ts.Parents()
			ts, err = n.ChainGetTipSet(ctx, tsk)
			if err != nil {
				return status, err
			}
		}

		status.ChainStatus.BlocksPerTipsetLastFinality = float64(blockCnt) / float64(build.Finality)

	}

	return status, nil
}

type AnchorHeightReply struct {
	Code int    `json:"code"`
	Err  string `json:"error"`

	Height uint64 `json:"height"`
	Blocks int    `json:"blocks"`
}

func httpCallAnchor(url string, timeout int, height uint64) AnchorHeightReply {
	result := AnchorHeightReply{}

	client := http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	url = fmt.Sprintf("%s?height=%d", url, height)
	resp, err := client.Get(url)
	if err != nil {
		log.Errorf("httpCallAnchor failed:%v, url:%s", err, url)
		return result
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("httpCallAnchor read body failed:%v, url:%s", err, url)
		return result
	}

	if resp.StatusCode != 200 {
		log.Errorf("httpCallAnchor req failed, status %d != 200, url:%s", resp.StatusCode, url)
		return result
	}

	err = json.Unmarshal(bodyBytes, &result)
	if err != nil {
		log.Errorf("httpCallAnchor json decode failed:%v. body str:%s, url:%s", err, string(bodyBytes), url)
		return result
	}

	log.Infof("httpCallAnchor ok, height:%d, blocks:%d, url:%s", result.Height, result.Blocks, url)
	return result
}

func callAnchor(height uint64) {
	timeout := AnchorData.AnchorTimeout
	now := time.Now()
	var wg sync.WaitGroup
	var xresults = make([]AnchorHeightReply, len(AnchorData.AnchorURLs))
	for i, c := range AnchorData.AnchorURLs {
		wg.Add(1)

		var idx = i
		var c2 = c
		go func() {
			result := httpCallAnchor(c2, timeout, height)
			xresults[idx] = result
			wg.Done()
		}()
	}

	wg.Wait()

	var results = make([]AnchorHeightReply, 0, len(AnchorData.AnchorURLs))
	for _, r := range xresults {
		if r.Code == 200 {
			results = append(results, r)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Height > results[j].Height {
			return true
		}

		if results[i].Height < results[j].Height {
			return false
		}

		if results[i].Blocks > results[j].Blocks {
			return true
		}

		return false
	})

	took := time.Since(now)
	if len(results) > 0 {
		AnchorData.anchorBlkCount = results[0].Blocks
		AnchorData.anchorHeight = abi.ChainEpoch(results[0].Height)

		log.Infof("callAnchor ok, Height %d, blocks:%d, took:%s", AnchorData.anchorHeight, AnchorData.anchorBlkCount, took)
	} else {
		log.Errorf("callAnchor failed, len(results) == 0, took:%s", took)
	}
}

func (n *FullNodeAPI) AnchorBlocksCountByHeight(ctx context.Context, height abi.ChainEpoch) (int, error) {
	AnchorData.anchorLock.Lock()
	defer AnchorData.anchorLock.Unlock()

	if height < AnchorData.anchorHeight {
		return 0, fmt.Errorf("lotus current anchor height:%d > req %d", AnchorData.anchorHeight, height)
	}

	if height < AnchorData.anchorQueryHeight {
		return 0, fmt.Errorf("lotus current anchor query height:%d > req %d", AnchorData.anchorQueryHeight, height)
	}

	if height == AnchorData.anchorHeight {
		return AnchorData.anchorBlkCount, nil
	}

	if AnchorData.anchorQueryHeight != height {
		if AnchorData.HardcoreDelay > 0 {
			time.Sleep(time.Duration(AnchorData.HardcoreDelay) * time.Second)
		}

		callAnchor(uint64(height))

		// save query height, avoid sleep multiple times
		AnchorData.anchorQueryHeight = height
	}

	if height == AnchorData.anchorHeight {
		return AnchorData.anchorBlkCount, nil
	}

	return 0, fmt.Errorf("lotus current anchor height:%d != req %d after call to anchor, maybe call failed, check lotus log",
		AnchorData.anchorHeight, height)
}

var _ api.FullNode = &FullNodeAPI{}
