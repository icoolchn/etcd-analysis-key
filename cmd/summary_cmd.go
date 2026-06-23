package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/spf13/cobra"
)

var (
	summaryInput     string
	summaryKeysOnly  bool
	summaryPrefix    string
	summaryGroupDepth int
	summaryTop       int
	summarySort      string
	summaryPageSize  int
	summaryPageSleep time.Duration

	summaryMinCreateRev int64
	summaryMaxCreateRev int64
	summaryMinModRev    int64
	summaryMaxModRev    int64

	summaryFilter    string
	summaryFilterMin int
	summaryFilterMax int

	summaryWriteOut string
	summaryOutput   string
)

func NewSummaryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "summary",
		Short: "Aggregate etcd keys by prefix group (online or from a JSONL snapshot)",
		Long: `
Aggregate etcd keys into prefix groups and print the Top N.

Two modes:
  Offline (recommended for large clusters): read a JSONL snapshot produced by
    'look --keys-only --write-out=jsonl --output=keys.jsonl' without contacting
    etcd:
      summary --input=keys.jsonl --group-depth=2 --sort=count --top=50
  Online (small clusters / ad-hoc): scan etcd directly:
      summary --keys-only --group-depth=2 --sort=count --top=50

--group-depth=N groups by the first N path segments, e.g. for
/registry/pods/default/nginx: depth=2 -> /registry/pods.

Revision bounds gate the created-count / modified-count metrics (they do NOT
filter keys out of count/size stats):
  --min-create-revision / --max-create-revision
  --min-mod-revision    / --max-mod-revision

--filter=key|value|kv with --filter-min/--filter-max drops records by size
before aggregation (client-side; does not reduce server traffic in online mode).
--keys-only --filter=value|kv is rejected (no value to size).
`,
		Run: summaryFunc,
	}

	cmd.Flags().StringVar(&summaryInput, "input", "", "JSONL snapshot file (offline mode); empty means online scan")
	cmd.Flags().BoolVar(&summaryKeysOnly, "keys-only", false, "Online mode: only fetch key metadata (no value)")
	cmd.Flags().StringVar(&summaryPrefix, "prefix", "", "Online mode: only scan keys with the given prefix")
	cmd.Flags().IntVar(&summaryGroupDepth, "group-depth", 2, "Group by first N path segments")
	cmd.Flags().IntVar(&summaryTop, "top", 20, "Number of prefix groups to output")
	cmd.Flags().StringVar(&summarySort, "sort", "count", "Sort key: count, total-size, avg-size, max-size, max-version, latest-mod-revision, created-count, modified-count")
	cmd.Flags().IntVar(&summaryPageSize, "page-size", core.DefaultPageSize(), "Online mode: per-request page size")
	cmd.Flags().DurationVar(&summaryPageSleep, "page-sleep", 0, "Online mode: sleep between pages, e.g. 50ms")

	cmd.Flags().Int64Var(&summaryMinCreateRev, "min-create-revision", 0, "Count keys created at/after this revision toward created-count")
	cmd.Flags().Int64Var(&summaryMaxCreateRev, "max-create-revision", 0, "Count keys created at/before this revision toward created-count")
	cmd.Flags().Int64Var(&summaryMinModRev, "min-mod-revision", 0, "Count keys modified at/after this revision toward modified-count")
	cmd.Flags().Int64Var(&summaryMaxModRev, "max-mod-revision", 0, "Count keys modified at/before this revision toward modified-count")

	cmd.Flags().StringVar(&summaryFilter, "filter", "none", "Filter attribute before aggregation: none, key, value, kv (client-side; does not reduce server traffic)")
	cmd.Flags().IntVar(&summaryFilterMin, "filter-min", -1, "Filter min size")
	cmd.Flags().IntVar(&summaryFilterMax, "filter-max", -1, "Filter max size")

	cmd.Flags().StringVar(&summaryWriteOut, "write-out", "text", "Output format: text or json")
	cmd.Flags().StringVar(&summaryOutput, "output", "", "Write output to file instead of stdout")

	cmd.RegisterFlagCompletionFunc("sort", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"count", "total-size", "avg-size", "max-size", "max-version", "latest-mod-revision", "created-count", "modified-count"}, cobra.ShellCompDirectiveDefault
	})
	cmd.RegisterFlagCompletionFunc("write-out", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"text", "json"}, cobra.ShellCompDirectiveDefault
	})
	cmd.RegisterFlagCompletionFunc("filter", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"none", "key", "value", "kv"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func summaryFunc(cmd *cobra.Command, args []string) {
	if !core.ValidSortKey(summarySort) {
		core.Exit(fmt.Errorf("invalid --sort value: %q", summarySort))
	}
	if err := core.CheckFilterCombination(summaryKeysOnly, summaryFilter); err != nil {
		core.Exit(err)
	}

	metas, err := collectMetas()
	if err != nil {
		core.Exit(err)
	}

	metas = core.FilterMetas(metas, core.FilterConfig{Attribute: summaryFilter, Min: summaryFilterMin, Max: summaryFilterMax})

	cfg := core.SummaryConfig{
		GroupDepth:        summaryGroupDepth,
		Top:               summaryTop,
		SortBy:            summarySort,
		MinCreateRevision: summaryMinCreateRev,
		MaxCreateRevision: summaryMaxCreateRev,
		MinModRevision:    summaryMinModRev,
		MaxModRevision:    summaryMaxModRev,
	}
	groups := core.Summarize(metas, cfg)

	out := os.Stdout
	if summaryOutput != "" {
		f, err := os.OpenFile(summaryOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			core.Exit(err)
		}
		defer f.Close()
		out = f
	}

	if summaryWriteOut == "json" {
		printSummaryJSON(out, groups, len(metas))
		return
	}
	printSummaryText(out, groups, len(metas))
}

// collectMetas gathers KeyMeta records either from a JSONL file (offline) or
// by scanning etcd (online). Size filtering (online) happens here; offline
// filtering is applied by the caller via core.FilterMetas.
func collectMetas() ([]core.KeyMeta, error) {
	fc := core.FilterConfig{Attribute: summaryFilter, Min: summaryFilterMin, Max: summaryFilterMax}

	if summaryInput != "" {
		return core.ReadJSONL(summaryInput)
	}

	core.InitClient()
	scanOpts := []core.ScanOption{core.WithPageSize(summaryPageSize)}
	if summaryPrefix != "" {
		scanOpts = append(scanOpts, core.WithPrefix(summaryPrefix))
	}
	if summaryKeysOnly {
		scanOpts = append(scanOpts, core.WithKeysOnly())
	}
	if summaryPageSleep > 0 {
		scanOpts = append(scanOpts, core.WithPageSleep(summaryPageSleep))
	}

	_, datac := core.ScanData(scanOpts...)
	var metas []core.KeyMeta
	for data := range datac {
		for _, kv := range data {
			if fc.Reject(kv) {
				continue
			}
			metas = append(metas, core.KVToMeta(kv, summaryKeysOnly, false))
		}
	}
	return metas, nil
}

func printSummaryText(out *os.File, groups []core.GroupStats, total int) {
	fmt.Fprintf(out, "Summary: %d keys, %d groups (top %d by %s)\n", total, len(groups), len(groups), summarySort)
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Group | Count | TotalSize | AvgSize | MaxSize | MaxVersion | LatestModRev | CreatedCount | ModifiedCount")
	hasSize := len(groups) > 0 && groups[0].HasSize
	for _, g := range groups {
		if hasSize {
			fmt.Fprintf(out, "%s | %d | %s | %s | %s | %d | %d | %d | %d\n",
				g.Group, g.Count,
				core.ReadableSize(int(g.TotalSize)), core.ReadableSize(int(g.AvgSize())), core.ReadableSize(int(g.MaxSize)),
				g.MaxVersion, g.LatestModRevision, g.CreatedCount, g.ModifiedCount)
		} else {
			fmt.Fprintf(out, "%s | %d | - | - | - | %d | %d | %d | %d\n",
				g.Group, g.Count,
				g.MaxVersion, g.LatestModRevision, g.CreatedCount, g.ModifiedCount)
		}
	}
}

func printSummaryJSON(out *os.File, groups []core.GroupStats, total int) {
	type groupOut struct {
		Group             string `json:"group"`
		Count             int    `json:"count"`
		TotalSize         int64  `json:"total_size_bytes"`
		AvgSize           int64  `json:"avg_size_bytes"`
		MaxSize           int64  `json:"max_size_bytes"`
		MaxVersion        int64  `json:"max_version"`
		LatestModRevision int64  `json:"latest_mod_revision"`
		CreatedCount      int64  `json:"created_count"`
		ModifiedCount     int64  `json:"modified_count"`
	}
	type report struct {
		Total int        `json:"total_keys"`
		Sort  string     `json:"sort_by"`
		Top   int        `json:"top"`
		Rows  []groupOut `json:"rows"`
	}

	rows := make([]groupOut, 0, len(groups))
	for _, g := range groups {
		rows = append(rows, groupOut{
			Group:             g.Group,
			Count:             g.Count,
			TotalSize:         g.TotalSize,
			AvgSize:           g.AvgSize(),
			MaxSize:           g.MaxSize,
			MaxVersion:        g.MaxVersion,
			LatestModRevision: g.LatestModRevision,
			CreatedCount:      g.CreatedCount,
			ModifiedCount:     g.ModifiedCount,
		})
	}
	r := report{Total: total, Sort: summarySort, Top: summaryTop, Rows: rows}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		core.Exit(err)
	}
	fmt.Fprintln(out, string(b))
}
