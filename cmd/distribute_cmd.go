package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
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
		Long: `
Show the data distribution of etcd.

According to setting <type>, this command will show the data distribution by the size of the <type>.
The 'kv' means the 'key' and 'value'.

According to setting <bucket>, this command will show the different size histogram.
Each size interval is '(maxSize - minSize) / bucket'.

According to the output below, it means:
when the data size is '0.0 B', the count of this kind of data is 12.
when the data size is greater than '0.0B' and less than or equal to '573.0 B', the count is 275.
'573.0 B' < size <= '1.1 KiB', count 80.

Example:
$ distribute --type=value --bucket=8
Summary:
  Count:        399.
  Total:        267.9 KiB.
  Smallest:     0.0 B.
  Largest:      4.5 KiB.
  Average:      687.0 B.

Size histogram:
  0.0 B [12]    |∎
  573.0 B [275] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
  1.1 KiB [80]   |∎∎∎∎∎∎∎∎∎∎
  1.7 KiB [0]    |
  2.2 KiB [0]    |
  2.8 KiB [0]    |
  3.4 KiB [0]    |
  3.9 KiB [0]    |
  4.5 KiB [32]   |∎∎∎∎

Size distribution:
  10% in 3.0 B.
  25% in 4.0 B.
  50% in 424.0 B.
  75% in 854.0 B.
  90% in 1.0 KiB.
  95% in 4.5 KiB.
  99% in 4.5 KiB.
`,
		Run: distributeFunc,
	}

	cmd.Flags().StringVar(&distributeType, "type", "key", "Distribution basis; key, value or kv")
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
		r = core.NewReport(bucketCount, sizeOf)
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
		if !isJSON && len(datac) > 0 {
			r.DynamicOutput()
		}
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

	// online current revision from the cached endpoint-status response.
	var curRev int64
	if st := core.LastStatus(); st != nil && st.Header != nil {
		curRev = st.Header.Revision
	}
	g := core.ComputeGlobalStats(metas, curRev)

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
			return int(m.ValueSize())
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
		r = core.NewReport(bucketCount, sizeOf)
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

	// offline: no endpoint status; current_revision falls back to max(mod_revision).
	g := core.ComputeGlobalStats(metas, 0)

	if isJSON {
		fmt.Println(distributeJSON(r, g))
		return
	}
	printDistributeText(r, g)
}

// printDistributeText renders the full distribute output per template 3.1.2.
// The existing size-distribution Report already printed itself to stdout during
// Run() (via its uilive writer + finalString); we append the remaining sections
// in order. Template 3.1.2 puts Overview first, but reordering would require
// buffering the size report, so we keep Report's native streaming output and
// follow it with Overview + Version + Count + Diagnosis.
//
// Compact-revision / revision-gap / compact-count are omitted (v3.5.x
// StatusResponse has no CompactRevision); mod_revision_age is skipped.
func printDistributeText(r core.Report, g core.GlobalStats) {
	// (size distribution already printed by Report during Run())

	fmt.Println()
	fmt.Println("=== Overview ===")
	fmt.Printf("  Total keys:           %s\n", core.FormatThousands(int64(g.TotalKeys)))
	if g.HasSize {
		fmt.Printf("  Total size:           %s\n", core.ReadableSize(int(g.TotalSize)))
		fmt.Printf("  Avg size:             %s\n", core.ReadableSize(int(g.AvgSize)))
		fmt.Printf("  Size p50 / p99:       %s / %s\n", core.ReadableSize(g.SizeP50), core.ReadableSize(g.SizeP99))
	}
	fmt.Printf("  Lease=0 (persistent): %.1f%%\n", g.LeaseZeroPct)
	if g.CurrentRevision > 0 {
		fmt.Printf("  Current revision:     %s  (source: %s)\n",
			core.FormatThousands(g.CurrentRevision), g.CurrentRevisionSource)
	}
	fmt.Printf("  create_revision min / max:  %s / %s\n",
		core.FormatThousands(g.CreateRevMin), core.FormatThousands(g.CreateRevMax))
	fmt.Printf("  mod_revision min / max:     %s / %s\n",
		core.FormatThousands(g.ModRevMin), core.FormatThousands(g.ModRevMax))

	fmt.Println()
	fmt.Println("=== Version Distribution ===")
	fmt.Println("Version histogram (log buckets):")
	maxVC := 0
	for _, b := range g.VersionBuckets {
		if b.Count > maxVC {
			maxVC = b.Count
		}
	}
	for _, b := range g.VersionBuckets {
		barLen := 0
		if maxVC > 0 {
			barLen = b.Count * 40 / maxVC
		}
		fmt.Printf("  %-8s [%s] |%s\n", b.Label, core.FormatThousands(int64(b.Count)), strings.Repeat("∎", barLen))
	}

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
	fmt.Println("  (mod_revision_age: skipped — needs reliable current_revision baseline)")
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
	type overviewJSON struct {
		TotalKeys             int64   `json:"total_keys"`
		TotalSizeBytes        int64   `json:"total_size_bytes,omitempty"`
		AvgSizeBytes          int64   `json:"avg_size_bytes,omitempty"`
		SizeP50               int     `json:"size_p50_bytes,omitempty"`
		SizeP99               int     `json:"size_p99_bytes,omitempty"`
		LeaseZeroPct          float64 `json:"lease_zero_pct,omitempty"`
		CurrentRevision       int64   `json:"current_revision,omitempty"`
		CurrentRevisionSource string  `json:"current_revision_source,omitempty"`
		CreateRevMin          int64   `json:"create_revision_min,omitempty"`
		CreateRevMax          int64   `json:"create_revision_max,omitempty"`
		ModRevMin             int64   `json:"mod_revision_min,omitempty"`
		ModRevMax             int64   `json:"mod_revision_max,omitempty"`
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
		SizeReport   json.RawMessage   `json:"size_report"`
		Overview     overviewJSON      `json:"overview"`
		VersionDist  []versionBucketJSON `json:"version_distribution"`
		CountConcentration []prefixJSON `json:"count_concentration"`
		Diagnosis    diagnosisJSON     `json:"diagnosis"`
	}

	// Parse the existing report JSON so we can nest it; field renaming to
	// template 2.4.3 (total_size_bytes / min/max/avg_size_bytes) is a follow-up
	// in core/report.go — here we embed as-is to avoid a double-encode.
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
			TotalKeys:             int64(g.TotalKeys),
			TotalSizeBytes:        g.TotalSize,
			AvgSizeBytes:          g.AvgSize,
			SizeP50:               g.SizeP50,
			SizeP99:               g.SizeP99,
			LeaseZeroPct:          g.LeaseZeroPct,
			CurrentRevision:       g.CurrentRevision,
			CurrentRevisionSource: g.CurrentRevisionSource,
			CreateRevMin:          g.CreateRevMin,
			CreateRevMax:          g.CreateRevMax,
			ModRevMin:             g.ModRevMin,
			ModRevMax:             g.ModRevMax,
		},
		VersionDist:         vb,
		CountConcentration:  pc,
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
