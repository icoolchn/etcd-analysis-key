package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
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
	cmd.Flags().StringVar(&summarySort, "sort", "count", "Sort key: count, total-size, avg-size, max-size, max-version, latest-mod-revision, created-count, modified-count, rev-count, tombstone-count")
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
		return []string{"count", "total-size", "avg-size", "max-size", "max-version", "latest-mod-revision", "created-count", "modified-count", "rev-count", "tombstone-count"}, cobra.ShellCompDirectiveDefault
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
//
// Offline prefix filtering: when --input and --prefix are both set, records
// whose key does not start with --prefix are dropped here. Without this, the
// offline branch would silently ignore --prefix (the online branch pushes it
// to the server via WithPrefix), making drill-down like
// `summary --input=keys.jsonl --prefix=/registry/events/kyuubi` a no-op.
func collectMetas() ([]core.KeyMeta, error) {
	fc := core.FilterConfig{Attribute: summaryFilter, Min: summaryFilterMin, Max: summaryFilterMax}

	if summaryInput != "" {
		metas, err := core.ReadJSONL(summaryInput)
		if err != nil {
			return nil, err
		}
		return core.FilterMetasByPrefix(metas, summaryPrefix), nil
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
	shown := len(groups)
	others := 0
	if shown > 0 && groups[shown-1].IsOthers() {
		others = groups[shown-1].OthersCount
		shown--
	}
	groupsLabel := formatThousands(total)
	if others > 0 {
		groupsLabel = fmt.Sprintf("%d (top %d + %d others)", total, shown, others)
	}
	fmt.Fprintf(out, "Summary: %s keys, %s groups by %s\n",
		formatThousands(total), groupsLabel, summarySort)
	fmt.Fprintln(out)

	hasRevCount := false
	for _, g := range groups {
		if g.RevCount > 0 || g.TombstoneCount > 0 {
			hasRevCount = true
			break
		}
	}
	hasSize := len(groups) > 0 && groups[0].HasSize

	// percent is only meaningful for additive sort dims (count / total-size).
	// For other dims we render "-" per the display template 3.2 percent rules.
	showPercent := summarySort == "count" || summarySort == "total-size"

	// Totals for percent. groups already includes the "others" row (whose
	// Count/TotalSize already aggregate the dropped tail), so summing here
	// yields the true full-set totals.
	var totalCount int
	var totalSize int64
	for _, g := range groups {
		totalCount += g.Count
		totalSize += g.TotalSize
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	header := "prefix\tcount\ttotal_size\tavg_size\tmax_size\tmax_version\tlatest_mod_revision\tcreated_count\tmodified_count"
	if hasRevCount {
		header += "\trev_count\ttombstone_count"
	}
	if showPercent {
		header += "\tpercent"
	}
	fmt.Fprintln(tw, header)

	for _, g := range groups {
		count := formatThousands(g.Count)
		totalSizeStr := "-"
		avgStr := "-"
		maxStr := "-"
		maxVer := "-"
		latestMod := "-"
		createdStr := "-"
		modifiedStr := "-"
		revStr := "-"
		tombStr := "-"

		if g.IsOthers() {
			// others row: only additive aggregates are meaningful (template 3.2).
			if hasSize {
				totalSizeStr = core.ReadableSize(int(g.TotalSize))
			}
			createdStr = formatThousands(int(g.CreatedCount))
			modifiedStr = formatThousands(int(g.ModifiedCount))
			if hasRevCount {
				revStr = formatThousands(int(g.RevCount))
				tombStr = formatThousands(int(g.TombstoneCount))
			}
		} else {
			if hasSize {
				totalSizeStr = core.ReadableSize(int(g.TotalSize))
				avgStr = core.ReadableSize(int(g.AvgSize()))
				maxStr = core.ReadableSize(int(g.MaxSize))
			}
			maxVer = fmt.Sprintf("%d", g.MaxVersion)
			latestMod = fmt.Sprintf("%d", g.LatestModRevision)
			createdStr = formatThousands(int(g.CreatedCount))
			modifiedStr = formatThousands(int(g.ModifiedCount))
			if hasRevCount {
				revStr = fmt.Sprintf("%d", g.RevCount)
				tombStr = fmt.Sprintf("%d", g.TombstoneCount)
			}
		}

		row := fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
			g.Group, count, totalSizeStr, avgStr, maxStr, maxVer, latestMod, createdStr, modifiedStr)
		if hasRevCount {
			row += fmt.Sprintf("\t%s\t%s", revStr, tombStr)
		}
		if showPercent {
			row += "\t" + percentStr(g, totalCount, totalSize)
		}
		fmt.Fprintln(tw, row)
	}
	tw.Flush()
}

// percentStr renders the percent column for one group per template 3.2:
// count sort -> group_count/total_count; total-size sort -> group_size/total_size.
func percentStr(g core.GroupStats, totalCount int, totalSize int64) string {
	if summarySort == "count" {
		if totalCount == 0 {
			return "-"
		}
		return fmt.Sprintf("%.1f%%", float64(g.Count)*100.0/float64(totalCount))
	}
	if summarySort == "total-size" {
		if totalSize == 0 {
			return "-"
		}
		return fmt.Sprintf("%.1f%%", float64(g.TotalSize)*100.0/float64(totalSize))
	}
	return "-"
}

// formatThousands renders an int with thousands separators (e.g. 1936675 -> 1,936,675).
func formatThousands(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if len(s) > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	return b.String()
}

func printSummaryJSON(out *os.File, groups []core.GroupStats, total int) {
	type groupOut struct {
		Group             string  `json:"group"`
		Count             int     `json:"count"`
		TotalSize         int64   `json:"total_size_bytes"`
		AvgSize           int64   `json:"avg_size_bytes"`
		MaxSize           int64   `json:"max_size_bytes"`
		MaxVersion        int64   `json:"max_version"`
		LatestModRevision int64   `json:"latest_mod_revision"`
		CreatedCount      int64   `json:"created_count"`
		ModifiedCount     int64   `json:"modified_count"`
		RevCount          int64   `json:"rev_count,omitempty"`
		TombstoneCount    int64   `json:"tombstone_count,omitempty"`
		Percent           float64 `json:"percent,omitempty"`
		IsOthers          bool    `json:"is_others,omitempty"`
	}
	type report struct {
		Total int        `json:"total_keys"`
		Sort  string     `json:"sort_by"`
		Top   int        `json:"top"`
		Rows  []groupOut `json:"rows"`
	}

	// Totals for percent. groups already includes the "others" row whose
	// Count/TotalSize aggregate the dropped tail.
	var totalCount int
	var totalSize int64
	for _, g := range groups {
		totalCount += g.Count
		totalSize += g.TotalSize
	}

	rows := make([]groupOut, 0, len(groups))
	for _, g := range groups {
		row := groupOut{
			Group:             g.Group,
			Count:             g.Count,
			TotalSize:         g.TotalSize,
			AvgSize:           g.AvgSize(),
			MaxSize:           g.MaxSize,
			MaxVersion:        g.MaxVersion,
			LatestModRevision: g.LatestModRevision,
			CreatedCount:      g.CreatedCount,
			ModifiedCount:     g.ModifiedCount,
			RevCount:          g.RevCount,
			TombstoneCount:    g.TombstoneCount,
		}
		if g.IsOthers() {
			row.IsOthers = true
			// avg/max/version/revision are not meaningful for the merge row.
			row.AvgSize = 0
			row.MaxSize = 0
			row.MaxVersion = 0
			row.LatestModRevision = 0
		}
		// percent: numeric, no "%" suffix (template 3.2). Only for additive dims.
		if summarySort == "count" && totalCount > 0 {
			row.Percent = float64(g.Count) / float64(totalCount)
		} else if summarySort == "total-size" && totalSize > 0 {
			row.Percent = float64(g.TotalSize) / float64(totalSize)
		}
		rows = append(rows, row)
	}
	r := report{Total: total, Sort: summarySort, Top: summaryTop, Rows: rows}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		core.Exit(err)
	}
	fmt.Fprintln(out, string(b))
}
