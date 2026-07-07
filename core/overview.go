package core

import (
	"fmt"
	"sort"
)

// GlobalStats aggregates cluster-wide metrics from per-key KeyMeta records,
// backing the `distribute` Overview + Diagnosis sections per
// etcd-analysis-key-data-display-template.md 3.1.
//
// Size metrics (TotalSize / AvgSize / SizeP50 / SizeP99 / SizeMin / SizeMax)
// are computed on the basis selected by --type (key / value / kv): the same
// basis drives both the Overview size fields and the Size Distribution
// histogram, so the two stay consistent. When the basis is unavailable
// (e.g. --type=value or --type=kv on a keys-only JSONL where value_size is
// absent), HasSize is false and size fields render as "-".
//
// Compact-revision / compact-count / revision-gap and current_revision are
// intentionally absent: v3.5.x StatusResponse does not carry CompactRevision,
// and the template (3.1.3) trimmed the Overview to scale + time-range only.
type GlobalStats struct {
	TotalKeys int

	// Size stats on all three bases, always populated independently so the
	// Overview can show key / value / kv side by side regardless of --type.
	// --type only selects which basis the Size Distribution histogram uses.
	// Each is zero-valued (Available=false) when that basis is unavailable
	// (e.g. value/kv on a keys-only JSONL where value_size is absent).
	KeySize   SizeStats
	ValueSize SizeStats
	KvSize    SizeStats

	// SizeBasis is which basis the Size Distribution histogram is on
	// (driven by --type, default "kv"). The histogram itself is rendered by
	// core.Report, not stored here.
	SizeBasis string

	LeaseZero    int
	LeaseZeroPct float64

	CreateRevMin int64
	CreateRevMax int64
	ModRevMin   int64
	ModRevMax   int64

	// history_revisions / tombstone_count totals (offline snapshot JSONL only;
	// online Range API does not return historical revision counts).
	// history_revisions = per-key historical revision count (NOT reset on
	// delete, unlike mvccpb.KeyValue.Version); tombstone_count = per-key
	// tombstone count. HasRevCount is true when at least one record carried
	// these fields. No distribution / no diagnosis threshold — just the global
	// sum, shown in the Overview so the global view does not silently lose
	// write-amplification signal offline.
	HistoryRevisionsTotal int64
	TombstoneCountTotal  int64
	HasRevCount          bool

	// version distribution: log buckets 1, 2~10, 11~100, 101~1K, 1K~10K, 10K+
	VersionBuckets []VersionBucket
	MaxVersion     int64
	// VersionPctls = [p10,p25,p50,p75,p90,p95,p99] of version, for the
	// "Version distribution" percentile table mirroring the size one.
	VersionPctls []int

	// count concentration: top-5 depth-2 prefix groups + others
	TopPrefixes []PrefixCount
	OthersCount int

	// Diagnosis (4 dims; mod_revision_age skipped — version dist covers write hotspots)
	Diagnosis Diagnosis
}

// SizeStats holds the aggregate size metrics on one basis (key/value/kv).
type SizeStats struct {
	Available bool // false when this basis is unavailable for all records
	Total     int64
	Min       int64
	Max       int64
	P50       int
	P99       int
}

// VersionBucket is one log-scale bucket of the version histogram.
type VersionBucket struct {
	Label string
	Count int
}

// PrefixCount is a depth-2 prefix group with its key count, used for count
// concentration.
type PrefixCount struct {
	Prefix string
	Count  int
	Pct    float64
}

// Diagnosis holds the one-line diagnostic verdicts per template 3.1.5.
type Diagnosis struct {
	Count   string // "CONCENTRATED" | "SKEWED" | "BALANCED"
	Size    string // "LARGE VALUES" | "OK"
	Version string // "WRITE HOTSPOT" | "OK"
	Lease   string // "HIGH PERSISTENT" | "OK"

	// Human-readable detail strings for each verdict.
	CountDetail   string
	SizeDetail    string
	VersionDetail string
	LeaseDetail   string
}

// ComputeGlobalStats aggregates metas into a GlobalStats. sizeBasis only
// selects which basis the Size Distribution histogram (rendered by core.Report)
// is on; the Overview always shows all three bases (key/value/kv).
func ComputeGlobalStats(metas []KeyMeta, sizeBasis string) GlobalStats {
	if sizeBasis == "" {
		sizeBasis = "kv"
	}
	g := GlobalStats{TotalKeys: len(metas), SizeBasis: sizeBasis}

	if len(metas) == 0 {
		return g
	}

	// Three independent size accumulators, one per basis. Each tracks its own
	// distinct-size list + count map for percentile computation.
	keyAcc := newSizeAccum()
	valAcc := newSizeAccum()
	kvAcc := newSizeAccum()
	versionAcc := newSizeAccum() // version as a size-like series for percentiles
	versionBuckets := newVersionBuckets()
	prefixCount := make(map[string]int)

	for _, m := range metas {
		keyAcc.add(int64(m.KeySizeBytes))
		valAcc.add(m.ValueSize())
		kvAcc.add(m.KvSize())
		versionAcc.add(m.Version)

		// lease
		if m.Lease == 0 {
			g.LeaseZero++
		}

		// history_revisions / tombstone_count (offline snapshot only).
		if m.RevCount != nil {
			g.HasRevCount = true
			g.HistoryRevisionsTotal += int64(*m.RevCount)
		}
		if m.TombstoneCount != nil {
			g.TombstoneCountTotal += int64(*m.TombstoneCount)
		}

		// revisions
		if g.CreateRevMin == 0 || m.CreateRevision < g.CreateRevMin {
			if m.CreateRevision > 0 || g.CreateRevMin == 0 {
				g.CreateRevMin = m.CreateRevision
			}
		}
		if m.CreateRevision > g.CreateRevMax {
			g.CreateRevMax = m.CreateRevision
		}
		if g.ModRevMin == 0 || m.ModRevision < g.ModRevMin {
			if m.ModRevision > 0 || g.ModRevMin == 0 {
				g.ModRevMin = m.ModRevision
			}
		}
		if m.ModRevision > g.ModRevMax {
			g.ModRevMax = m.ModRevision
		}

		// version
		if m.Version > g.MaxVersion {
			g.MaxVersion = m.Version
		}
		versionBuckets.add(m.Version)

		// count concentration by depth-2 prefix
		prefixCount[GroupPrefix(m.Key, 2)]++
	}

	g.KeySize = keyAcc.finalize()
	g.ValueSize = valAcc.finalize()
	g.KvSize = kvAcc.finalize()
	g.VersionPctls = versionAcc.finalizePctls()

	if g.TotalKeys > 0 {
		g.LeaseZeroPct = float64(g.LeaseZero) * 100.0 / float64(g.TotalKeys)
	}

	g.VersionBuckets = versionBuckets.buckets

	// count concentration: top-5 + others
	g.TopPrefixes, g.OthersCount = topPrefixes(prefixCount, 5)

	g.Diagnosis = computeDiagnosis(g)
	return g
}

// sizeAccum collects per-basis size totals + distinct sizes for percentiles.
type sizeAccum struct {
	available   bool
	total       int64
	min         int64
	max         int64
	sizes       []int // distinct sizes, for percentiles
	sizeToCount map[int]int
	seen        map[int]bool
}

func newSizeAccum() *sizeAccum {
	return &sizeAccum{
		min:         -1,
		sizeToCount: make(map[int]int),
		seen:        make(map[int]bool),
	}
}

// add accumulates one record's size on this basis. s < 0 means the basis is
// unavailable for this record (e.g. value/kv on a keys-only record); skip.
func (a *sizeAccum) add(s int64) {
	if s < 0 {
		return
	}
	a.available = true
	a.total += s
	if a.min < 0 || s < a.min {
		a.min = s
	}
	if s > a.max {
		a.max = s
	}
	// dedup: percentiles expects one entry per distinct size.
	if !a.seen[int(s)] {
		a.seen[int(s)] = true
		a.sizes = append(a.sizes, int(s))
	}
	a.sizeToCount[int(s)]++
}

// finalize returns the SizeStats, computing p50/p99 via percentiles.
func (a *sizeAccum) finalize() SizeStats {
	if !a.available {
		return SizeStats{}
	}
	sort.Ints(a.sizes)
	pctls := percentiles(a.sizes, a.sizeToCount)
	s := SizeStats{Available: true, Total: a.total, Min: a.min, Max: a.max}
	if len(pctls) >= 7 {
		s.P50 = pctls[2]
		s.P99 = pctls[6]
	}
	return s
}

// finalizePctls returns the full percentile slice [p10,p25,p50,p75,p90,p95,p99],
// used for the version distribution table.
func (a *sizeAccum) finalizePctls() []int {
	if !a.available {
		return nil
	}
	sort.Ints(a.sizes)
	return percentiles(a.sizes, a.sizeToCount)
}

// versionHist is a log-scale version histogram builder.
type versionHist struct {
	buckets []VersionBucket
}

func newVersionBuckets() *versionHist {
	return &versionHist{
		buckets: []VersionBucket{
			{Label: "1"},
			{Label: "2~10"},
			{Label: "11~100"},
			{Label: "101~1K"},
			{Label: "1K~10K"},
			{Label: "10K+"},
		},
	}
}

func (h *versionHist) add(v int64) {
	switch {
	case v <= 1:
		h.buckets[0].Count++
	case v <= 10:
		h.buckets[1].Count++
	case v <= 100:
		h.buckets[2].Count++
	case v <= 1000:
		h.buckets[3].Count++
	case v <= 10000:
		h.buckets[4].Count++
	default:
		h.buckets[5].Count++
	}
}

func topPrefixes(m map[string]int, topN int) ([]PrefixCount, int) {
	type kv struct {
		k string
		v int
	}
	all := make([]kv, 0, len(m))
	for k, v := range m {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v > all[j].v })

	total := 0
	for _, x := range all {
		total += x.v
	}

	n := topN
	if n > len(all) {
		n = len(all)
	}
	out := make([]PrefixCount, 0, n+1)
	shown := 0
	for i := 0; i < n; i++ {
		pct := 0.0
		if total > 0 {
			pct = float64(all[i].v) * 100.0 / float64(total)
		}
		out = append(out, PrefixCount{Prefix: all[i].k, Count: all[i].v, Pct: pct})
		shown += all[i].v
	}
	if n < len(all) {
		othersCount := total - shown
		pct := 0.0
		if total > 0 {
			pct = float64(othersCount) * 100.0 / float64(total)
		}
		out = append(out, PrefixCount{
			Prefix: fmt.Sprintf("others (%d)", len(all)-n),
			Count:  othersCount,
			Pct:    pct,
		})
		return out, len(all) - n
	}
	return out, 0
}

// computeDiagnosis applies the template 3.1.5 thresholds for the 4 supported
// dimensions. mod_revision_age is skipped (needs a reliable current_revision
// baseline, which is approximate offline).
func computeDiagnosis(g GlobalStats) Diagnosis {
	d := Diagnosis{}

	// count: top-1 >= 80% -> CONCENTRATED; top-3 >= 90% -> SKEWED; else BALANCED.
	if len(g.TopPrefixes) > 0 && g.TopPrefixes[0].Prefix != "" {
		top1 := g.TopPrefixes[0].Pct
		top3 := 0.0
		for i := 0; i < 3 && i < len(g.TopPrefixes); i++ {
			top3 += g.TopPrefixes[i].Pct
		}
		switch {
		case top1 >= 80:
			d.Count = "CONCENTRATED"
			d.CountDetail = fmt.Sprintf("top-1 = %.1f%%, %s", top1, g.TopPrefixes[0].Prefix)
		case top3 >= 90:
			d.Count = "SKEWED"
			d.CountDetail = fmt.Sprintf("top-3 = %.1f%%", top3)
		default:
			d.Count = "BALANCED"
			d.CountDetail = fmt.Sprintf("top-1 = %.1f%%", top1)
		}
	}

	// size: kv p99 >= 100KiB -> LARGE VALUES. Diagnosis always uses kv (not the
	// --type basis) so the threshold doesn't drift with --type.
	if g.KvSize.Available {
		if g.KvSize.P99 >= 100*1024 {
			d.Size = "LARGE VALUES"
		} else {
			d.Size = "OK"
		}
		d.SizeDetail = fmt.Sprintf("kv p99=%s, max=%s", ReadableSize(g.KvSize.P99), ReadableSize(int(g.KvSize.Max)))
	} else {
		d.Size = "OK"
		d.SizeDetail = "keys-only, no kv size"
	}

	// version: any version > 1K -> WRITE HOTSPOT.
	if g.MaxVersion > 1000 {
		d.Version = "WRITE HOTSPOT"
	} else {
		d.Version = "OK"
	}
	d.VersionDetail = fmt.Sprintf("max version=%s", formatThousandsInt64(g.MaxVersion))

	// lease: lease=0 >= 20% -> HIGH PERSISTENT.
	if g.TotalKeys > 0 {
		if g.LeaseZeroPct >= 20 {
			d.Lease = "HIGH PERSISTENT"
		} else {
			d.Lease = "OK"
		}
		d.LeaseDetail = fmt.Sprintf("%.1f%% lease=0 (persistent)", g.LeaseZeroPct)
	}

	return d
}

func formatThousandsInt64(n int64) string {
	return FormatThousands(n)
}
