package main

import (
	_ "net/http/pprof"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	lcli "github.com/filecoin-project/lotus/cli"
)

var pauseWorkerCmd = &cli.Command{
	Name:  "worker-pause",
	Usage: "pause a worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "uuid",
			Usage: "specify the worker session uuid",
		},
	},

	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		uuid := cctx.String("uuid")
		if uuid == "" {
			return xerrors.Errorf("must specify a valid uuid")
		}

		err = nodeApi.WorkerPause(lcli.ReqContext(cctx), uuid)
		if err != nil {
			return err
		}

		return nil
	},
}

var resumeWorkerCmd = &cli.Command{
	Name:  "worker-resume",
	Usage: "resume a worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "uuid",
			Usage: "specify the worker session uuid",
		},
	},

	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		uuid := cctx.String("uuid")
		if uuid == "" {
			return xerrors.Errorf("must specify a valid uuid")
		}

		err = nodeApi.WorkerResume(lcli.ReqContext(cctx), uuid)
		if err != nil {
			return err
		}

		return nil
	},
}
