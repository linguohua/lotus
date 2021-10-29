package impl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
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
var AnchorData = AnchorData2{}

type AnchorData2 struct {
	AnchorURL     string
	AnchorTimeout int

	anchorLock     sync.Mutex
	anchorHeight   abi.ChainEpoch
	anchorBlkCount int
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

type AnchorRespBlock struct {
	Miner string `json:"Miner"`
}

type AnchorRespData struct {
	Height int64              `json:"Height"`
	Blocks []*AnchorRespBlock `json:"Blocks"`
}

type AnchorResp struct {
	Result *AnchorRespData `json:"result"`
}

func callAnchor() {
	timeout := 3
	if AnchorData.AnchorTimeout > 0 {
		timeout = AnchorData.AnchorTimeout
	}

	client := http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	url := "https://api.node.glif.io/rpc/v0"
	if AnchorData.AnchorURL != "" {
		url = AnchorData.AnchorURL
	}

	jsonStr := "{ \"jsonrpc\": \"2.0\", \"method\": \"Filecoin.ChainHead\", \"params\": [], \"id\": 1 }"
	resp, err := client.Post(url, "application/json", bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		log.Errorf("callAnchor failed:%v, url:%s", err, url)
		return
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("callAnchor read body failed:%v", err)
		return
	}

	if resp.StatusCode != 200 {
		log.Errorf("callAnchor req failed, status %d != 200", resp.StatusCode)
		return
	}

	aresp := &AnchorResp{}
	err = json.Unmarshal(bodyBytes, aresp)
	if err != nil {
		log.Errorf("callAnchor json decode failed:%v", err)
		return
	}

	if aresp.Result != nil {
		AnchorData.anchorBlkCount = len(aresp.Result.Blocks)
		AnchorData.anchorHeight = abi.ChainEpoch(aresp.Result.Height)
		log.Infof("callAnchor ok, aresp.Result Height %d, blocks:%d", AnchorData.anchorHeight, AnchorData.anchorBlkCount)
	} else {
		log.Errorf("callAnchor failed, aresp.Result is nil")
	}
}

func (n *FullNodeAPI) AnchorBlocksCountByHeight(ctx context.Context, height abi.ChainEpoch) (int, error) {
	AnchorData.anchorLock.Lock()
	defer AnchorData.anchorLock.Unlock()

	if height < AnchorData.anchorHeight {
		return 0, fmt.Errorf("lotus current anchor height:%d > req %d", AnchorData.anchorHeight, height)
	}

	if height == AnchorData.anchorHeight {
		return AnchorData.anchorBlkCount, nil
	}

	callAnchor()

	if height == AnchorData.anchorHeight {
		return AnchorData.anchorBlkCount, nil
	}

	return 0, fmt.Errorf("lotus current anchor height:%d != req %d after call to anchor, maybe call failed, check lotus log",
		AnchorData.anchorHeight, height)
}

var _ api.FullNode = &FullNodeAPI{}
