package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	distributeType      string
	bucketCount         int
	distributeWriteOut  string
	distributePrefix    string
	distributeInput     string
	distributePageSize  int
	distributePageSleep time.Duration
)

func NewDistributeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "distribute",
		Short: "Show the data distribution of etcd",
		Long: `Show the global distribution of etcd data: Overview + Size Distribution +
Version Distribution + Count Concentration + Diagnosis.

Modes:
  Online (default): scan etcd via --endpoints.
      etcdctl+ distribute --type=kv
  Offline: read a KeyMeta JSONL exported by 'look --write-out=jsonl'.
      etcdctl+ distribute --input=keys.jsonl --type=kv
  Offline snapshot db values (rev_count / tombstone_count) are reflected in
  the Overview when present; online mode shows "online: unavailable" for those
  two rows (the etcd Range API does not return historical revision counts).

--type and size basis:
  --type controls which size the size fields and the Size Distribution
  histogram use. Overview size fields are labeled with the basis.
    kv    (default) len(key)+len(value)   -> "Total kv size", "Kv size p50/p99"
    key             len(key)              -> "Total key size", ...
    value           len(value)            -> "Total value size", ...
  When the basis is unavailable (e.g. --type=value on a keys-only JSONL),
  the size fields show "-" and the Size Distribution section shows a notice;
  Version / Count / Diagnosis are unaffected (they don't depend on value size).

--bucket controls the Size Distribution histogram bucket count (default 5).

Diagnosis rules (4 dims; mod_revision_age is not used — version distribution
covers write hotspots):

  dim     normal                warning                       follow-up
  count   top-1 < 80%           top-1 >= 80%  CONCENTRATED    summary --sort=count
  size    p99 < 100KiB          p99 >= 100KiB LARGE VALUES    summary --sort=total-size
  version max < 1K              exists > 1K   WRITE HOTSPOT   summary --sort=max-version
  lease   lease=0 < 20%         lease=0 >= 20% HIGH PERSISTENT look --filter="lease=0"

Output format: --write-out=text (default) / json.
`,
		Run: distributeFunc,
	}

	cmd.Flags().StringVar(&distributeType, "type", "kv", "Distribution basis; key, value or kv (default kv)")
	cmd.Flags().IntVar(&bucketCount, "bucket", 5, "Bucket Count")
	cmd.Flags().StringVar(&distributeWriteOut, "write-out", "text", "Output format: text or json")
	cmd.Flags().StringVar(&distributeInput, "input", "", "KeyMeta JSONL file (offline mode); empty means online scan")
	cmd.Flags().StringVar(&distributePrefix, "prefix", "", "Only scan keys with the given prefix (server-side)")
	cmd.Flags().IntVar(&distributePageSize, "page-size", core.DefaultPageSize(), "Per-request page size")
	cmd.Flags().DurationVar(&distributePageSleep, "page-sleep", 0, "Sleep between pages, e.g. 50ms")

	cmd.RegisterFlagCompletionFunc("write-out", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"text", "json"}, cobra.ShellCompDirectiveDefault
	})

	cmd.RegisterFlagCompletionFunc("type", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"key", "value", "kv"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func distributeFunc(cmd *cobra.Command, args []string) {
	isJSON := distributeWriteOut == "json"

	if distributeInput != "" {
		distributeFromJSONL(isJSON)
		return
	}

	core.InitClient()
	scanOpts := []core.ScanOption{core.WithPageSize(distributePageSize)}
	if distributePrefix != "" {
		scanOpts = append(scanOpts, core.WithPrefix(distributePrefix))
	}
	if distributePageSleep > 0 {
		scanOpts = append(scanOpts, core.WithPageSleep(distributePageSleep))
	}
	_, datac := core.ScanData(scanOpts...)

	sizeOf := func(kv *mvccpb.KeyValue) int {
		switch distributeType {
		case "value":
			return len(kv.Value)
		case "kv":
			return len(kv.Key) + len(kv.Value)
		case "key":
			fallthrough
		default:
			return len(kv.Key)
		}
	}

	var r core.Report
	if isJSON {
		r = core.NewReport(bucketCount, sizeOf, core.WithJSONMode())
	} else {
		// Silent: don't print the size report to stdout during Run; we embed it
		// in the template-ordered layout (Overview first) via String() instead.
		r = core.NewReport(bucketCount, sizeOf, core.WithSilent())
	}

	// Collect metas alongside the size report so GlobalStats (Overview /
	// version dist / count concentration / diagnosis) can be computed in a
	// single pass without re-scanning. distribute is already a full scan, so
	// holding per-key metadata is acceptable.
	var metas []core.KeyMeta
	var mu sync.Mutex

	c1 := r.Results()
	go func() {
		defer close(c1)
		// No DynamicOutput here: the silent Report does not print its own
		// finalString, and a live refresh would race with the template-ordered
		// layout (Overview first) we print after Run. The size report is
		// emitted once via r.String() inside printDistributeText.
		for data := range datac {
			c1 <- data
			// snapshot metas for GlobalStats (online distribute fetches values,
			// so KVToMeta with keysOnly=false gives full sizes).
			batch := make([]core.KeyMeta, 0, len(data))
			for _, kv := range data {
				batch = append(batch, core.KVToMeta(kv, false, false))
			}
			mu.Lock()
			metas = append(metas, batch...)
			mu.Unlock()
		}
	}()
	<-r.Run()

	// --type drives both the size histogram (sizeOf above) and the Overview size
	// fields, so they share one basis. Online distribute always fetches values,
	// so value/kv are available; --type=key is also fine.
	g := core.ComputeGlobalStats(metas, distributeType)

	if isJSON {
		fmt.Println(distributeJSON(r, g))
		return
	}
	printDistributeText(r, g)
}

// distributeFromJSONL runs distribute from a JSONL file (offline mode).
func distributeFromJSONL(isJSON bool) {
	metas, err := core.ReadJSONL(distributeInput)
	if err != nil {
		core.Exit(err)
	}

	metaSizeOf := func(m core.KeyMeta) int {
		switch distributeType {
		case "value":
			s := m.ValueSize()
			if s < 0 {
				return 0
			}
			return int(s)
		case "kv":
			s := m.KvSize()
			if s < 0 {
				return 0
			}
			return int(s)
		case "key":
			fallthrough
		default:
			return m.KeySizeBytes
		}
	}

	sizeOf := func(kv *mvccpb.KeyValue) int { return len(kv.Key) }
	var r core.Report
	if isJSON {
		r = core.NewReport(bucketCount, sizeOf, core.WithJSONMode())
	} else {
		r = core.NewReport(bucketCount, sizeOf, core.WithSilent())
	}

	c1 := r.Results()
	go func() {
		defer close(c1)
		batch := make([]*mvccpb.KeyValue, 0, 1000)
		for _, m := range metas {
			size := metaSizeOf(m)
			kv := &mvccpb.KeyValue{
				Key:   make([]byte, size),
				Value: nil,
			}
			batch = append(batch, kv)
			if len(batch) >= 1000 {
				c1 <- batch
				batch = make([]*mvccpb.KeyValue, 0, 1000)
			}
		}
		if len(batch) > 0 {
			c1 <- batch
		}
	}()
	<-r.Run()

	// offline: basis from --type. If the JSONL is keys-only (no value_size),
	// --type=value/kv yields HasSize=false and size fields render as "-".
	g := core.ComputeGlobalStats(metas, distributeType)

	if isJSON {
		fmt.Println(distributeJSON(r, g))
		return
	}
	printDistributeText(r, g)
}

// printDistributeText renders the full distribute output per template 3.1.2:
// Overview -> Size Distribution -> Version Distribution -> Count Concentration
// -> Diagnosis. The size report is retrieved via String() (Report ran silent)
// so we can place it after Overview instead of letting it stream first.
//
// mod_revision_age is skipped (version distribution covers write hotspots).
func printDistributeText(r core.Report, g core.GlobalStats) {
	fmt.Println("=== Overview ===")
	fmt.Printf("  Total keys:                    %s\n", core.FormatThousands(int64(g.TotalKeys)))
	printSizeGroup("key", g.KeySize, int64(g.TotalKeys))
	printSizeGroup("value", g.ValueSize, int64(g.TotalKeys))
	printSizeGroup("kv", g.KvSize, int64(g.TotalKeys))
	fmt.Println("  --- activity ---")
	fmt.Printf("  Lease=0 (persistent):          %.1f%%\n", g.LeaseZeroPct)
	fmt.Printf("  create_revision min / max:     %s / %s\n",
		core.FormatThousands(g.CreateRevMin), core.FormatThousands(g.CreateRevMax))
	fmt.Printf("  mod_revision min / max:         %s / %s\n",
		core.FormatThousands(g.ModRevMin), core.FormatThousands(g.ModRevMax))
	fmt.Printf("  max version:                   %s\n", core.FormatThousands(g.MaxVersion))
	if g.HasRevCount {
		fmt.Printf("  history_revisions (total):     %s\n", core.FormatThousands(g.HistoryRevisionsTotal))
		fmt.Printf("  tombstone_count (total):       %s\n", core.FormatThousands(g.TombstoneCountTotal))
	} else {
		fmt.Printf("  history_revisions (total):     online: unavailable\n")
		fmt.Printf("  tombstone_count (total):       online: unavailable\n")
	}

	fmt.Println()
	fmt.Printf("=== Size Distribution (%s) ===\n", g.SizeBasis)
	if sizeAvailable(g, g.SizeBasis) {
		fmt.Print(r.String())
	} else {
		fmt.Printf("  (unavailable: --type=%s requires value size, but the data is keys-only)\n", g.SizeBasis)
	}

	fmt.Println()
	fmt.Println("=== Version Distribution ===")
	fmt.Println("  Version histogram:")
	maxVC := 0
	for _, b := range g.VersionBuckets {
		if b.Count > maxVC {
			maxVC = b.Count
		}
	}
	vtw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, b := range g.VersionBuckets {
		barLen := 0
		if maxVC > 0 {
			barLen = b.Count * 40 / maxVC
		}
		fmt.Fprintf(vtw, "    %s\t[%s]\t|%s\n", b.Label, core.FormatThousands(int64(b.Count)), strings.Repeat("∎", barLen))
	}
	vtw.Flush()
	fmt.Println("  Version distribution:")
	printVersionPercentiles(g.VersionPctls)
	fmt.Println()

	fmt.Println("=== Count Concentration (top-5 depth-2 prefixes) ===")
	for _, p := range g.TopPrefixes {
		fmt.Printf("  %-50s %s  (%.1f%%)\n", p.Prefix, core.FormatThousands(int64(p.Count)), p.Pct)
	}

	fmt.Println()
	fmt.Println("=== Diagnosis ===")
	fmt.Printf("  count:     %s  (%s)\n", verdictMark(g.Diagnosis.Count), g.Diagnosis.CountDetail)
	fmt.Printf("  size:      %s  (%s)\n", verdictMark(g.Diagnosis.Size), g.Diagnosis.SizeDetail)
	fmt.Printf("  version:   %s  (%s)\n", verdictMark(g.Diagnosis.Version), g.Diagnosis.VersionDetail)
	fmt.Printf("  lease:     %s  (%s)\n", verdictMark(g.Diagnosis.Lease), g.Diagnosis.LeaseDetail)
}

// sizeAvailable reports whether the given basis has data (keys-only data has
// no value/kv, so those bases render the Size Distribution as unavailable).
func sizeAvailable(g core.GlobalStats, basis string) bool {
	switch basis {
	case "value":
		return g.ValueSize.Available
	case "key":
		return g.KeySize.Available
	case "kv":
		fallthrough
	default:
		return g.KvSize.Available
	}
}

// printSizeGroup prints one basis block of the Overview size section.
// When the basis is unavailable (keys-only data + value/kv), every field
// shows "-" instead of numbers.
func printSizeGroup(name string, s core.SizeStats, totalKeys int64) {
	fmt.Printf("  --- size (%s) ---\n", name)
	if !s.Available {
		fmt.Printf("  Total %s size:                -\n", name)
		fmt.Printf("  Avg %s size:                  -\n", name)
		fmt.Printf("  %s size min / max:            -\n", name)
		fmt.Printf("  %s size p50 / p99:            -\n", name)
		return
	}
	avg := int64(0)
	if totalKeys > 0 {
		avg = s.Total / totalKeys
	}
	fmt.Printf("  Total %s size:                %s\n", name, core.ReadableSize(int(s.Total)))
	fmt.Printf("  Avg %s size:                  %s\n", name, core.ReadableSize(int(avg)))
	fmt.Printf("  %s size min / max:            %s / %s\n", name, core.ReadableSize(int(s.Min)), core.ReadableSize(int(s.Max)))
	fmt.Printf("  %s size p50 / p99:            %s / %s\n", name, core.ReadableSize(s.P50), core.ReadableSize(s.P99))
}

// printVersionPercentiles renders the version distribution percentile table,
// mirroring the size "X% in Y." format. pctls is [p10,p25,p50,p75,p90,p95,p99].
func printVersionPercentiles(pctls []int) {
	labels := []int{10, 25, 50, 75, 90, 95, 99}
	for i, p := range labels {
		if i >= len(pctls) {
			break
		}
		fmt.Printf("    %d%% in %s.\n", p, core.FormatThousands(int64(pctls[i])))
	}
}

// verdictMark returns a one-glyph status marker for a diagnosis verdict.
func verdictMark(v string) string {
	switch v {
	case "OK":
		return "✅ OK         "
	case "BALANCED":
		return "✅ BALANCED   "
	default:
		return "⚠️  " + v
	}
}

// distributeJSON renders the combined JSON output: existing size report fields
// (renamed to template 2.4.3 alignment) plus Overview / version dist / count
// concentration / diagnosis.
func distributeJSON(r core.Report, g core.GlobalStats) string {
	type sizeStatsJSON struct {
		Available bool  `json:"available"`
		Total     int64 `json:"total_bytes"`
		Min       int64 `json:"min_bytes"`
		Max       int64 `json:"max_bytes"`
		P50       int   `json:"p50_bytes"`
		P99       int   `json:"p99_bytes"`
	}
	type overviewJSON struct {
		TotalKeys           int64        `json:"total_keys"`
		SizeBasis           string       `json:"size_basis"`
		KeySize             sizeStatsJSON `json:"key_size"`
		ValueSize           sizeStatsJSON `json:"value_size"`
		KvSize              sizeStatsJSON `json:"kv_size"`
		LeaseZeroPct        float64      `json:"lease_zero_pct"`
		CreateRevMin        int64        `json:"create_revision_min"`
		CreateRevMax        int64        `json:"create_revision_max"`
		ModRevMin           int64        `json:"mod_revision_min"`
		ModRevMax           int64        `json:"mod_revision_max"`
		MaxVersion          int64        `json:"max_version"`
		HasRevCount         bool         `json:"has_rev_count"`
		HistoryRevisions    int64        `json:"history_revisions_total,omitempty"`
		TombstoneCount      int64        `json:"tombstone_count_total,omitempty"`
		VersionPctls        []int        `json:"version_percentiles,omitempty"`
	}
	type versionBucketJSON struct {
		Label string `json:"label"`
		Count int    `json:"count"`
	}
	type prefixJSON struct {
		Prefix string  `json:"prefix"`
		Count  int     `json:"count"`
		Pct    float64 `json:"pct"`
	}
	type diagnosisJSON struct {
		Count   string `json:"count"`
		Size    string `json:"size"`
		Version string `json:"version"`
		Lease   string `json:"lease"`
	}
	type out struct {
		SizeReport         json.RawMessage     `json:"size_report"`
		Overview           overviewJSON        `json:"overview"`
		VersionDist        []versionBucketJSON `json:"version_distribution"`
		CountConcentration []prefixJSON        `json:"count_concentration"`
		Diagnosis          diagnosisJSON       `json:"diagnosis"`
	}

	toSizeJSON := func(s core.SizeStats) sizeStatsJSON {
		return sizeStatsJSON{Available: s.Available, Total: s.Total, Min: s.Min, Max: s.Max, P50: s.P50, P99: s.P99}
	}
	vb := make([]versionBucketJSON, 0, len(g.VersionBuckets))
	for _, b := range g.VersionBuckets {
		vb = append(vb, versionBucketJSON{Label: b.Label, Count: b.Count})
	}
	pc := make([]prefixJSON, 0, len(g.TopPrefixes))
	for _, p := range g.TopPrefixes {
		pc = append(pc, prefixJSON{Prefix: p.Prefix, Count: p.Count, Pct: p.Pct})
	}

	o := out{
		SizeReport: json.RawMessage(r.JSON()),
		Overview: overviewJSON{
			TotalKeys:        int64(g.TotalKeys),
			SizeBasis:        g.SizeBasis,
			KeySize:          toSizeJSON(g.KeySize),
			ValueSize:        toSizeJSON(g.ValueSize),
			KvSize:           toSizeJSON(g.KvSize),
			LeaseZeroPct:     g.LeaseZeroPct,
			CreateRevMin:    g.CreateRevMin,
			CreateRevMax:     g.CreateRevMax,
			ModRevMin:       g.ModRevMin,
			ModRevMax:       g.ModRevMax,
			MaxVersion:      g.MaxVersion,
			HasRevCount:     g.HasRevCount,
			HistoryRevisions:    g.HistoryRevisionsTotal,
			TombstoneCount:  g.TombstoneCountTotal,
			VersionPctls:    g.VersionPctls,
		},
		VersionDist:        vb,
		CountConcentration: pc,
		Diagnosis: diagnosisJSON{
			Count:   g.Diagnosis.Count,
			Size:    g.Diagnosis.Size,
			Version: g.Diagnosis.Version,
			Lease:   g.Diagnosis.Lease,
		},
	}
	b, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return fmt.Sprintf(`{"error":"marshal failed: %v"}`, err)
	}
	return string(b)
}
