package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/spf13/cobra"
)

var (
	walSummaryInput   string
	walSummaryDataDir string
	walSummarySort    string
	walSummaryTop     int
	walSummaryWriteOut string
	walSummaryOutput  string
)

func NewWalSummaryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wal-summary",
		Short: "Aggregate WAL operations by key",
		Long: `
Aggregate WAL operations by key and print the Top N.

Two modes:
  Offline (recommended): read WalOp JSONL produced by 'wal-look --write-out=jsonl':
    wal-summary --input=wal.jsonl --sort=put-count --top=50
  Direct: parse WAL files directly (slower, re-parses each time):
    wal-summary --data-dir=/var/lib/etcd --sort=put-count --top=50

Sort keys: put-count, delete-count, total-ops.
`,
		Run: walSummaryFunc,
	}

	cmd.Flags().StringVar(&walSummaryInput, "input", "", "WalOp JSONL file (offline); empty means parse WAL directly")
	cmd.Flags().StringVar(&walSummaryDataDir, "data-dir", "", "etcd data directory (required when --input is empty)")
	cmd.Flags().StringVar(&walSummarySort, "sort", "put-count", "Sort key: put-count, delete-count, total-ops")
	cmd.Flags().IntVar(&walSummaryTop, "top", 20, "Number of keys to output")
	cmd.Flags().StringVar(&walSummaryWriteOut, "write-out", "text", "Output format: text or json")
	cmd.Flags().StringVar(&walSummaryOutput, "output", "", "Write output to file instead of stdout")

	cmd.RegisterFlagCompletionFunc("sort", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"put-count", "delete-count", "total-ops"}, cobra.ShellCompDirectiveDefault
	})
	cmd.RegisterFlagCompletionFunc("write-out", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"text", "json"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func walSummaryFunc(cmd *cobra.Command, args []string) {
	var ops []core.WalOp
	var scope *core.WalScope
	var err error

	if walSummaryInput != "" {
		ops, err = core.ReadWalJSONL(walSummaryInput)
	} else if walSummaryDataDir != "" {
		ops, scope, err = core.CollectWalOpsWithScope(walSummaryDataDir)
	} else {
		core.Exit(fmt.Errorf("either --input or --data-dir is required"))
		return
	}
	if err != nil {
		core.Exit(err)
	}

	// In direct (--data-dir) mode, print the analysis-scope summary to stderr
	// first so the user knows what range was read. In --input mode the scope is
	// unknown (jsonl carries no snap/wal metadata), so nothing is printed.
	if scope != nil {
		core.PrintWalScope(os.Stderr, scope, len(ops))
	}

	stats := core.AggregateWalOps(ops)
	core.SortWalKeyStats(stats, walSummarySort)

	if walSummaryTop > 0 && walSummaryTop < len(stats) {
		stats = stats[:walSummaryTop]
	}

	dist := core.EntryTypeDist(ops)

	out := os.Stdout
	if walSummaryOutput != "" {
		f, err := os.OpenFile(walSummaryOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			core.Exit(err)
		}
		defer f.Close()
		out = f
	}

	if walSummaryWriteOut == "json" {
		printWalSummaryJSON(out, stats, len(ops), dist)
		return
	}
	printWalSummaryText(out, stats, len(ops), dist)
}

func printWalSummaryText(out *os.File, stats []core.WalKeyStats, totalOps int, dist []core.EntryTypeCount) {
	if len(dist) > 0 {
		fmt.Fprintf(out, "Entry-Type Distribution (%d total ops):\n", totalOps)
		for _, d := range dist {
			line := fmt.Sprintf("  %-20s %8d", d.EntryType, d.Count)
			if d.EntryType == "IRRTxn" {
				line += "  (parent; sub-ops already counted in IRRPut/IRRDeleteRange)"
			}
			fmt.Fprintln(out, line)
		}
		fmt.Fprintln(out)
	}
	fmt.Fprintf(out, "WAL Summary: %d total ops, %d unique keys (top %d by %s)\n\n", totalOps, len(stats), len(stats), walSummarySort)
	fmt.Fprintf(out, "%-60s %10s %12s %18s\n", "Key", "PutCount", "DeleteCount", "TotalValueSize")
	for _, s := range stats {
		fmt.Fprintf(out, "%-60s %10d %12d %18s\n",
			s.Key, s.PutCount, s.DeleteCount, core.ReadableSize(int(s.TotalValueSizeBytes)))
	}
}

func printWalSummaryJSON(out *os.File, stats []core.WalKeyStats, totalOps int, dist []core.EntryTypeCount) {
	type report struct {
		TotalOps      int                  `json:"total_ops"`
		EntryTypeDist []core.EntryTypeCount `json:"entry_type_dist"`
		SortBy        string               `json:"sort_by"`
		Top           int                  `json:"top"`
		Rows          []core.WalKeyStats   `json:"rows"`
	}
	r := report{
		TotalOps:      totalOps,
		EntryTypeDist: dist,
		SortBy:        walSummarySort,
		Top:           walSummaryTop,
		Rows:          stats,
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		core.Exit(err)
	}
	fmt.Fprintln(out, string(b))
}
