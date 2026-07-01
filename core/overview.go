package core

import (
	"fmt"
	"sort"
)

// GlobalStats aggregates cluster-wide metrics from per-key KeyMeta records,
// plus optional cluster metadata (current revision, db size) from endpoint
// status when online. It backs the `distribute` Overview + Diagnosis sections
// per etcd-analysis-key-data-display-template.md 3.1.
//
// Compact-revision / compact-count / revision-gap are intentionally absent:
// v3.5.x StatusResponse does not carry CompactRevision (it only appears on
// WatchResponse), so those three cannot be obtained via the clientv3 API.
// The template's "cluster health" row is therefore trimmed to current_revision
// only; compact metrics would require reading the snapshot db meta bucket or
// Prometheus metrics, which is a separate data channel.
type GlobalStats struct {
	TotalKeys int
	TotalSize int64
	HasSize   bool // false when all records are keys-only (no value size)

	LeaseZero    int
	LeaseZeroPct float64

	SizeP50  int
	SizeP99  int
	SizeMin  int64
	SizeMax  int64
	AvgSize  int64

	CreateRevMin int64
	CreateRevMax int64
	ModRevMin   int64
	ModRevMax   int64

	// current revision baseline: online = endpoint status Header.Revision;
	// offline = max(mod_revision) across records. 0 when no records.
	CurrentRevision int64
	CurrentRevisionSource string // "endpoint status" | "max(mod_revision)" | ""

	// version distribution: log buckets 1, 2~10, 11~100, 101~1K, 1K~10K, 10K+
	VersionBuckets []VersionBucket
	MaxVersion     int64

	// count concentration: top-5 depth-2 prefix groups + others
	TopPrefixes []PrefixCount
	OthersCount int

	// Diagnosis (4 dims; mod_revision_age skipped per plan)
	Diagnosis Diagnosis
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
// mod_revision_age is intentionally omitted (needs a reliable current_revision
// baseline and is noisy offline).
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

// ComputeGlobalStats aggregates metas into a GlobalStats. currentRevision is
// the online endpoint-status revision (0 if offline); when 0, the offline
// fallback max(mod_revision) is used and CurrentRevisionSource reflects that.
func ComputeGlobalStats(metas []KeyMeta, currentRevision int64) GlobalStats {
	g := GlobalStats{TotalKeys: len(metas)}

	if len(metas) == 0 {
		return g
	}

	sizes := make([]int, 0, len(metas))
	sizeToCount := make(map[int]int)
	seenSize := make(map[int]bool)
	versionBuckets := newVersionBuckets()
	prefixCount := make(map[string]int)

	g.SizeMin = -1
	for _, m := range metas {
		// size
		if kv := m.KvSize(); kv >= 0 {
			g.HasSize = true
			g.TotalSize += kv
			if g.SizeMin < 0 || kv < g.SizeMin {
				g.SizeMin = kv
			}
			if kv > g.SizeMax {
				g.SizeMax = kv
			}
			// dedup sizes: percentiles expects one entry per distinct size
			// (matching Report.processResult), not one per record.
			if !seenSize[int(kv)] {
				seenSize[int(kv)] = true
				sizes = append(sizes, int(kv))
			}
			sizeToCount[int(kv)]++
		}

		// lease
		if m.Lease == 0 {
			g.LeaseZero++
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

	if g.HasSize && g.TotalKeys > 0 {
		g.AvgSize = g.TotalSize / int64(g.TotalKeys)
		g.LeaseZeroPct = float64(g.LeaseZero) * 100.0 / float64(g.TotalKeys)
		// percentiles expects sizes sorted ascending (it does not sort internally).
		sort.Ints(sizes)
		pctls := percentiles(sizes, sizeToCount)
		// pctls order: 10,25,50,75,90,95,99
		if len(pctls) >= 7 {
			g.SizeP50 = pctls[2]
			g.SizeP99 = pctls[6]
		}
	}

	g.VersionBuckets = versionBuckets.buckets

	// current revision baseline
	if currentRevision > 0 {
		g.CurrentRevision = currentRevision
		g.CurrentRevisionSource = "endpoint status"
	} else if g.ModRevMax > 0 {
		g.CurrentRevision = g.ModRevMax
		g.CurrentRevisionSource = "max(mod_revision)"
	}

	// count concentration: top-5 + others
	g.TopPrefixes, g.OthersCount = topPrefixes(prefixCount, 5)

	g.Diagnosis = computeDiagnosis(g)
	return g
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

	// size: p99 >= 100KiB -> LARGE VALUES.
	if g.HasSize {
		if g.SizeP99 >= 100*1024 {
			d.Size = "LARGE VALUES"
		} else {
			d.Size = "OK"
		}
		d.SizeDetail = fmt.Sprintf("p99=%s, max=%s", ReadableSize(g.SizeP99), ReadableSize(int(g.SizeMax)))
	} else {
		d.Size = "OK"
		d.SizeDetail = "keys-only, no size"
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
