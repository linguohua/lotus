package cli

import (
	"fmt"
	_ "net/http/pprof"
	"sort"

	"github.com/fatih/color"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
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

	tasktype := cctx.String("tt")
	if tasktype == "" {
		return xerrors.Errorf("must specify a valid tasktype")
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
			err = nodeApi.WorkerPause(ReqContext(cctx), uuid, tasktype)
			if err != nil {
				return err
			}
		} else {
			err = nodeApi.WorkerResume(ReqContext(cctx), uuid, tasktype)
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
		&cli.StringFlag{
			Name:  "tt",
			Usage: "specify task type to pause: ap,p1,p2,c1,c2,fin,all",
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
		&cli.StringFlag{
			Name:  "tt",
			Usage: "specify task type to resume: ap,p1,p2,c1,c2,fin,all",
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

			if len(stat.Paused) > 0 {
				paused = color.RedString(" (paused:" + stat.Paused + ")")
			}

			fmt.Printf("Worker %s, host %s%s%s, group:%s, url:%s\n", stat.id,
				color.MagentaString(stat.Info.Hostname), disabled, paused, stat.Info.GroupID, stat.Url)

			fmt.Printf("Task type:\n")
			for i, tt := range stat.TaskTypes {
				fmt.Printf("%s: %d\n", tt, stat.TaskCounts[i])
			}

		}

		return nil
	},
}

var RemoveWorkerCmd = &cli.Command{
	Name:  "worker-remove",
	Usage: "Remove a worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "uuid",
			Usage: "specify the worker session uuid",
		},
	},

	Action: func(cctx *cli.Context) error {
		return workerRemove(cctx, false)
	},
}

func workerRemove(cctx *cli.Context, v bool) error {
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
		err = nodeApi.WorkerRemove(ReqContext(cctx), uuid)
		if err != nil {
			return err
		}
	}

	return nil
}

var UpdateP1ParamsCmd = &cli.Command{
	Name:  "update-params-p1",
	Usage: "update p1 params",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "tickets",
			Usage: "specify tickets per interval",
		},
		&cli.UintFlag{
			Name:  "interval",
			Usage: "specify interval in minutes",
		},
	},

	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		tickets := cctx.Uint("tickets")
		interval := cctx.Uint("interval")

		if tickets < 1 {
			return xerrors.Errorf("tickets must not < 1")
		}

		if interval < 1 {
			return xerrors.Errorf("interval must not < 1")
		}

		return nodeApi.UpdateP1TicketsParams(ReqContext(cctx), tickets, interval)
	},
}

var UpdateFinParamsCmd = &cli.Command{
	Name:  "update-params-fin",
	Usage: "update finalize params",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "tickets",
			Usage: "specify tickets per interval",
		},
		&cli.UintFlag{
			Name:  "interval",
			Usage: "specify interval in minutes, 0 means disable",
		},
	},

	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		tickets := cctx.Uint("tickets")
		if tickets < 1 {
			return xerrors.Errorf("tickets must not < 1")
		}

		interval := cctx.Uint("interval")

		return nodeApi.UpdateFinalizeTicketsParams(ReqContext(cctx), tickets, interval)
	},
}
