package ffiwrapper

import (
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("ffiwrapper")

type cacheClearFunc func(cache string, size uint64)
type Sealer struct {
	sectors  SectorProvider
	stopping chan struct{}

	merkleTreecache string
	ccfunc          cacheClearFunc
}

func (sb *Sealer) Stop() {
	close(sb.stopping)
}
