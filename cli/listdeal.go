package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var StateDumpDealsCmd = &cli.Command{
	Name:  "dump-deals",
	Usage: "dump deals to file",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		ts, err := LoadTipSet(ctx, cctx, api)
		if err != nil {
			return err
		}

		filepath := cctx.Args().First()
		if filepath == "" {
			return fmt.Errorf("should provide output file path")
		}

		count, err := api.StateMarketDealsDump(ctx, ts.Key(), filepath)
		if err != nil {
			return err
		}

		fmt.Printf("dump total %d deals to file:%s\n", count, filepath)

		return nil
	},
}
