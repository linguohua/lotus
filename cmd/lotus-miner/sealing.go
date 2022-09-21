package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"

	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/httpreader"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

var sealingCmd = &cli.Command{
	Name:  "sealing",
	Usage: "interact with sealing pipeline",
	Subcommands: []*cli.Command{
		sealingJobsCmd,
		workersCmd(true),
		sealingSchedDiagCmd,
		sealingAbortCmd,
		sealingDataCidCmd,
		sealingNextSectorIDCmd,
	},
}

func workersCmd(sealing bool) *cli.Command {
	return &cli.Command{
		Name:  "workers",
		Usage: "list workers",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "color",
				Usage:       "use color in display output",
				DefaultText: "depends on output being a TTY",
			},
		},
		Action: func(cctx *cli.Context) error {
			if cctx.IsSet("color") {
				color.NoColor = !cctx.Bool("color")
			}

			nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
			if err != nil {
				return err
			}
			defer closer()

			ctx := lcli.ReqContext(cctx)

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
				if len(stat.Tasks) > 0 {
					if (stat.Tasks[0].WorkerType() != sealtasks.WorkerSealing) == sealing {
						continue
					}
				}

				st = append(st, sortableStat{id, stat})
			}

			sort.Slice(st, func(i, j int) bool {
				return st[i].id.String() < st[j].id.String()
			})

			/*
				Example output:

				Worker c4d65451-07f8-4230-98ad-4f33dea2a8cc, host myhostname
				        TASK: PC1(1/4) AP(15/15) GET(3)
				        CPU:  [||||||||                                                        ] 16/128 core(s) in use
				        RAM:  [||||||||                                                        ] 12% 125.8 GiB/1008 GiB
				        VMEM: [||||||||                                                        ] 12% 125.8 GiB/1008 GiB
				        GPU:  [                                                                ] 0% 0.00/1 gpu(s) in use
				        GPU: NVIDIA GeForce RTX 3090, not used
			*/

			//for _, stat := range st {
			// gpuUse := "not "
			// gpuCol := color.FgBlue
			// if stat.GpuUsed > 0 {
			// 	gpuCol = color.FgGreen
			// 	gpuUse = ""
			// }

			//var disabled string
			//if !stat.Enabled {
			//	disabled = color.RedString(" (disabled)")
			//}

			//fmt.Printf("Worker %s, host %s%s\n", stat.id, color.MagentaString(stat.Info.Hostname), disabled)

			// fmt.Printf("\tCPU:  [%s] %d/%d core(s) in use\n",
			// 	barString(float64(stat.Info.Resources.CPUs), 0, float64(stat.CpuUse)), stat.CpuUse, stat.Info.Resources.CPUs)

			// ramTotal := stat.Info.Resources.MemPhysical
			// ramTasks := stat.MemUsedMin
			// ramUsed := stat.Info.Resources.MemUsed
			// var ramReserved uint64 = 0
			// if ramUsed > ramTasks {
			// 	ramReserved = ramUsed - ramTasks
			// }
			// ramBar := barString(float64(ramTotal), float64(ramReserved), float64(ramTasks))

			// fmt.Printf("\tRAM:  [%s] %d%% %s/%s\n", ramBar,
			// 	(ramTasks+ramReserved)*100/stat.Info.Resources.MemPhysical,
			// 	types.SizeStr(types.NewInt(ramTasks+ramUsed)),
			// 	types.SizeStr(types.NewInt(stat.Info.Resources.MemPhysical)))

			// vmemTotal := stat.Info.Resources.MemPhysical + stat.Info.Resources.MemSwap
			// vmemTasks := stat.MemUsedMax
			// vmemUsed := stat.Info.Resources.MemUsed + stat.Info.Resources.MemSwapUsed
			// var vmemReserved uint64 = 0
			// if vmemUsed > vmemTasks {
			// 	vmemReserved = vmemUsed - vmemTasks
			// }
			// vmemBar := barString(float64(vmemTotal), float64(vmemReserved), float64(vmemTasks))

			// fmt.Printf("\tVMEM: [%s] %d%% %s/%s\n", vmemBar,
			// 	(vmemTasks+vmemReserved)*100/vmemTotal,
			// 	types.SizeStr(types.NewInt(vmemTasks+vmemReserved)),
			// 	types.SizeStr(types.NewInt(vmemTotal)))

			// if len(stat.Info.Resources.GPUs) > 0 {
			// 	gpuBar := barString(float64(len(stat.Info.Resources.GPUs)), 0, stat.GpuUsed)
			// 	fmt.Printf("\tGPU:  [%s] %.f%% %.2f/%d gpu(s) in use\n", color.GreenString(gpuBar),
			// 		stat.GpuUsed*100/float64(len(stat.Info.Resources.GPUs)),
			// 		stat.GpuUsed, len(stat.Info.Resources.GPUs))
			// }
			// for _, gpu := range stat.Info.Resources.GPUs {
			// 	fmt.Printf("\tGPU: %s\n", color.New(gpuCol).Sprintf("%s, %sused", gpu, gpuUse))
			// }
			//}
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
}

var sealingJobsCmd = &cli.Command{
	Name:  "jobs",
	Usage: "list running jobs",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
		&cli.BoolFlag{
			Name:  "show-ret-done",
			Usage: "show returned but not consumed calls",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		jobs, err := nodeApi.WorkerJobs(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker jobs: %w", err)
		}

		type line struct {
			storiface.WorkerJob
			wid uuid.UUID
		}

		lines := make([]line, 0)

		for wid, jobs := range jobs {
			for _, job := range jobs {
				lines = append(lines, line{
					WorkerJob: job,
					wid:       wid,
				})
			}
		}

		// oldest first
		sort.Slice(lines, func(i, j int) bool {
			if lines[i].RunWait != lines[j].RunWait {
				return lines[i].RunWait < lines[j].RunWait
			}
			if lines[i].Start.Equal(lines[j].Start) {
				return lines[i].ID.ID.String() < lines[j].ID.ID.String()
			}
			return lines[i].Start.Before(lines[j].Start)
		})

		workerHostnames := map[uuid.UUID]string{}

		wst, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker stats: %w", err)
		}

		for wid, st := range wst {
			workerHostnames[wid] = st.Info.Hostname
		}

		tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "ID\tSector\tWorker\tHostname\tTask\tState\tTime\n")

		for _, l := range lines {
			state := "running"
			switch {
			case l.RunWait > 1:
				state = fmt.Sprintf("assigned(%d)", l.RunWait-1)
			case l.RunWait == storiface.RWPrepared:
				state = "prepared"
			case l.RunWait == storiface.RWRetDone:
				if !cctx.Bool("show-ret-done") {
					continue
				}
				state = "ret-done"
			case l.RunWait == storiface.RWReturned:
				state = "returned"
			case l.RunWait == storiface.RWRetWait:
				state = "ret-wait"
			}
			dur := "n/a"
			if !l.Start.IsZero() {
				dur = time.Now().Sub(l.Start).Truncate(time.Millisecond * 100).String()
			}

			hostname, ok := workerHostnames[l.wid]
			if !ok {
				hostname = l.Hostname
			}

			_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
				hex.EncodeToString(l.ID.ID[:4]),
				l.Sector.Number,
				hex.EncodeToString(l.wid[:4]),
				hostname,
				l.Task.Short(),
				state,
				dur)
		}

		return tw.Flush()
	},
}

var sealingSchedDiagCmd = &cli.Command{
	Name:  "sched-diag",
	Usage: "Dump internal scheduler state",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name: "force-sched",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		st, err := nodeApi.SealingSchedDiag(ctx, cctx.Bool("force-sched"))
		if err != nil {
			return err
		}

		j, err := json.MarshalIndent(&st, "", "  ")
		if err != nil {
			return err
		}

		fmt.Println(string(j))

		return nil
	},
}

var sealingAbortCmd = &cli.Command{
	Name:      "abort",
	Usage:     "Abort a running job",
	ArgsUsage: "[callid]",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "sched",
			Usage: "Specifies that the argument is UUID of the request to be removed from scheduler",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("expected 1 argument")
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		if cctx.Bool("sched") {
			err = nodeApi.SealingRemoveRequest(ctx, uuid.Must(uuid.Parse(cctx.Args().First())))
			if err != nil {
				return xerrors.Errorf("Failed to removed the request with UUID %s: %w", cctx.Args().First(), err)
			}
			return nil
		}

		jobs, err := nodeApi.WorkerJobs(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker jobs: %w", err)
		}

		var job *storiface.WorkerJob
	outer:
		for _, workerJobs := range jobs {
			for _, j := range workerJobs {
				if strings.HasPrefix(j.ID.ID.String(), cctx.Args().First()) {
					j := j
					job = &j
					break outer
				}
			}
		}

		if job == nil {
			return xerrors.Errorf("job with specified id prefix not found")
		}

		fmt.Printf("aborting job %s, task %s, sector %d, running on host %s\n", job.ID.String(), job.Task.Short(), job.Sector.Number, job.Hostname)

		return nodeApi.SealingAbort(ctx, job.ID)
	},
}

var sealingNextSectorIDCmd = &cli.Command{
	Name:      "nextid",
	Usage:     "set next sector id to datastore, make sure miner is not running",
	ArgsUsage: "[sector id]",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 1 {
			return nil
		}

		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("expected 1 argument")
		}

		var nextIDStr = cctx.Args().First()
		nextIDint, err := strconv.Atoi(nextIDStr)
		if err != nil {
			return err
		}

		repoPath := cctx.String(FlagMinerRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}

		if !ok {
			return xerrors.Errorf("repo at '%s' is not initialized", cctx.String(FlagMinerRepo))
		}

		lr, err := r.Lock(repo.StorageMiner)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck

		ctx := lcli.ReqContext(cctx)
		mds, err := lr.Datastore(ctx, "/metadata")
		if err != nil {
			return err
		}

		buf2, err := mds.Get(ctx, datastore.NewKey(modules.StorageCounterDSPrefix))
		if err == nil {
			currentNextID, err := binary.ReadUvarint(bytes.NewReader(buf2))
			if err != nil {
				return xerrors.Errorf("binary read current next id failed:%v", err)
			} else {
				fmt.Printf("current next id:%d\n", currentNextID)
			}
		} else {
			fmt.Printf("read current next id from datastore failed:%v", err)
		}

		var nextID = abi.SectorNumber(nextIDint)
		fmt.Printf("set new next id:%d\n", nextID)

		buf := make([]byte, binary.MaxVarintLen64)
		size := binary.PutUvarint(buf, uint64(nextID))
		return mds.Put(ctx, datastore.NewKey(modules.StorageCounterDSPrefix), buf[:size])
	},
}

var sealingDataCidCmd = &cli.Command{
	Name:      "data-cid",
	Usage:     "Compute data CID using workers",
	ArgsUsage: "[file/url] <padded piece size>",
	Flags: []cli.Flag{
		&cli.Uint64Flag{
			Name:  "file-size",
			Usage: "real file size",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 1 || cctx.Args().Len() > 2 {
			return xerrors.Errorf("expected 1 or 2 arguments")
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		var r io.Reader
		sz := cctx.Uint64("file-size")

		if strings.HasPrefix(cctx.Args().First(), "http://") || strings.HasPrefix(cctx.Args().First(), "https://") {
			r = &httpreader.HttpReader{
				URL: cctx.Args().First(),
			}

			if !cctx.IsSet("file-size") {
				resp, err := http.Head(cctx.Args().First())
				if err != nil {
					return xerrors.Errorf("http head: %w", err)
				}

				if resp.ContentLength < 0 {
					return xerrors.Errorf("head response didn't contain content length; specify --file-size")
				}
				sz = uint64(resp.ContentLength)
			}
		} else {
			p, err := homedir.Expand(cctx.Args().First())
			if err != nil {
				return xerrors.Errorf("expanding path: %w", err)
			}

			f, err := os.OpenFile(p, os.O_RDONLY, 0)
			if err != nil {
				return xerrors.Errorf("opening source file: %w", err)
			}

			if !cctx.IsSet("file-size") {
				st, err := f.Stat()
				if err != nil {
					return xerrors.Errorf("stat: %w", err)
				}
				sz = uint64(st.Size())
			}

			r = f
		}

		var psize abi.PaddedPieceSize
		if cctx.Args().Len() == 2 {
			rps, err := humanize.ParseBytes(cctx.Args().Get(1))
			if err != nil {
				return xerrors.Errorf("parsing piece size: %w", err)
			}
			psize = abi.PaddedPieceSize(rps)
			if err := psize.Validate(); err != nil {
				return xerrors.Errorf("checking piece size: %w", err)
			}
			if sz > uint64(psize.Unpadded()) {
				return xerrors.Errorf("file larger than the piece")
			}
		} else {
			psize = padreader.PaddedSize(sz).Padded()
		}

		pc, err := nodeApi.ComputeDataCid(ctx, psize.Unpadded(), r)
		if err != nil {
			return xerrors.Errorf("computing data CID: %w", err)
		}

		fmt.Println(pc.PieceCID, " ", pc.Size)
		return nil
	},
}
