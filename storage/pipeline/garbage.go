package sealing

import (
	"context"

	"golang.org/x/xerrors"

	abi "github.com/filecoin-project/go-state-types/abi"
	api "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

func (m *Sealing) PledgeSector(ctx context.Context) (storiface.SectorRef, error) {
	m.startupWait.Wait()

	m.inputLk.Lock()
	defer m.inputLk.Unlock()

	cfg, err := m.getConfig()
	if err != nil {
		return storiface.SectorRef{}, xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() >= cfg.MaxSealingSectors {
			return storiface.SectorRef{}, xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	spt, err := m.currentSealProof(ctx)
	if err != nil {
		return storiface.SectorRef{}, xerrors.Errorf("getting seal proof type: %w", err)
	}

	sid, err := m.createSector(ctx, cfg, spt)
	if err != nil {
		return storiface.SectorRef{}, err
	}

	log.Infof("Creating CC sector %d", sid)
	return m.minerSector(spt, sid), m.sectors.Send(uint64(sid), SectorStartCC{
		ID:         sid,
		SectorType: spt,
	})
}

func (m *Sealing) RecoverSector(ctx context.Context, sid abi.SectorNumber) (storiface.SectorRef, error) {
	m.startupWait.Wait()

	m.inputLk.Lock()
	defer m.inputLk.Unlock()

	if m.sealingDisable {
		return storiface.SectorRef{}, xerrors.Errorf("Miner RecoverSector failed, miner is sealing disable")
	}

	if !m.recoverMode {
		return storiface.SectorRef{}, xerrors.Errorf("Miner RecoverSector failed, miner is not in recover mode")
	}

	info, err := m.SectorsStatus(ctx, sid, false)
	if err != nil {
		return storiface.SectorRef{}, xerrors.Errorf("Miner getting recover sector info: %w", err)
	}

	if info.State != api.SectorState(Proving) {
		return storiface.SectorRef{}, xerrors.Errorf("Miner recover sector failed, sector state %s no Proving:", info.State)
	}

	spt, err := m.currentSealProof(ctx)
	if err != nil {
		return storiface.SectorRef{}, xerrors.Errorf("getting seal proof type: %w", err)
	}

	log.Infof("Recover CC sector %d", sid)
	return m.minerSector(spt, sid), m.sectors.Send(uint64(sid), SectorRedoPacked{})
}
