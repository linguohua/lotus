package cli

import (
	"fmt"
	_ "net/http/pprof"
	"sort"

	"github.com/fatih/color"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

func workerPause(cctx *cli.Context, v bool) error {
	nodeApi, closer, err := GetStorageMinerAPI(cctx)
	if err != nil {
		return err
	}
	defer closer()
	uuid := cctx.String("uuid")
	if uuid == "" {
		return xerrors.Errorf("must specify a valid uuid")
	}

	uuids := make([]string, 0, 16)
	if uuid == "all" {
		stats, err := nodeApi.WorkerStats(ReqContext(cctx))
		if err != nil {
			return err
		}
		for _, st := range stats {
			uuids = append(uuids, st.UUID)
		}
	} else {
		uuids = append(uuids, uuid)
	}

	for _, uuid := range uuids {
		if v {
			err = nodeApi.WorkerPause(ReqContext(cctx), uuid)
			if err != nil {
				return err
			}
		} else {
			err = nodeApi.WorkerResume(ReqContext(cctx), uuid)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

var PauseWorkerCmd = &cli.Command{
	Name:  "worker-pause",
	Usage: "pause a worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "uuid",
			Usage: "specify the worker session uuid",
		},
	},

	Action: func(cctx *cli.Context) error {
		return workerPause(cctx, true)
	},
}

var ResumeWorkerCmd = &cli.Command{
	Name:  "worker-resume",
	Usage: "resume a worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "uuid",
			Usage: "specify the worker session uuid",
		},
	},

	Action: func(cctx *cli.Context) error {
		return workerPause(cctx, false)
	},
}

var ListWorkersCmd = &cli.Command{
	Name:  "workers",
	Usage: "list workers",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "color"},
	},
	Action: func(cctx *cli.Context) error {
		color.NoColor = !cctx.Bool("color")

		nodeApi, closer, err := GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		stats, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return err
		}

		type sortableStat struct {
			id uuid.UUID
			storiface.WorkerStats
		}

		st := make([]sortableStat, 0, len(stats))
		for id, stat := range stats {
			st = append(st, sortableStat{id, stat})
		}

		sort.Slice(st, func(i, j int) bool {
			return st[i].id.String() < st[j].id.String()
		})

		for _, stat := range st {
			var disabled string
			var paused string
			if !stat.Enabled {
				disabled = color.RedString(" (disabled)")
			}

			if !stat.Paused {
				paused = color.RedString(" (paused)")
			}

			fmt.Printf("Worker %s, host %s%s%s group:%s\n", stat.id,
				color.MagentaString(stat.Info.Hostname), disabled, paused, stat.Info.GroupID)
			fmt.Printf("url: %s\n", stat.Url)
			fmt.Printf("Task type:\n")

			for i, tt := range stat.TaskTypes {
				fmt.Printf("%s: %d\n", tt, stat.TaskCounts[i])
			}

		}

		return nil
	},
}
