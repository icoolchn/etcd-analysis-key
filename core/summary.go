package core

import (
	"fmt"
	"sort"
	"strings"
)

// SummaryConfig controls how a set of KeyMeta records is aggregated.
type SummaryConfig struct {
	GroupDepth        int
	Top               int
	SortBy            string
	MinCreateRevision int64
	MaxCreateRevision int64
	MinModRevision    int64
	MaxModRevision    int64
}

// GroupStats holds aggregated metrics for one prefix group.
type GroupStats struct {
	Group             string
	Count             int
	TotalSize         int64 // sum of kv_size; 0 when sizes unknown (keys-only)
	HasSize           bool
	MaxSize           int64
	MaxVersion        int64
	LatestModRevision int64
	CreatedCount      int64 // keys whose create_revision is within [min,max] bounds
	ModifiedCount     int64 // keys whose mod_revision is within [min,max] bounds
	RevCount          int64 // sum of rev_count (offline snapshot only)
	TombstoneCount    int64 // sum of tombstone_count (offline snapshot only)

	// OthersCount is the number of dropped groups merged into this "others" row.
	// It is 0 for normal rows; >0 only for the synthetic others row appended by
	// Summarize when --top truncates the result. Per template 3.2, avg_size /
	// max_version / max_mod_revision render as "-" for the others row.
	OthersCount int
}

// IsOthers reports whether this row is the synthetic "others" merge row.
func (g GroupStats) IsOthers() bool { return g.OthersCount > 0 }

// AvgSize returns the mean kv_size for the group, or 0 when sizes unknown.
func (g GroupStats) AvgSize() int64 {
	if !g.HasSize || g.Count == 0 {
		return 0
	}
	return g.TotalSize / int64(g.Count)
}

// Valid sort keys.
var summarySortKeys = map[string]bool{
	"count":               true,
	"total-size":          true,
	"avg-size":            true,
	"max-size":            true,
	"max-version":         true,
	"latest-mod-revision": true,
	"created-count":       true,
	"modified-count":      true,
	"rev-count":           true,
	"tombstone-count":     true,
}

// ValidSortKey reports whether key is a supported --sort value.
func ValidSortKey(key string) bool {
	return summarySortKeys[key]
}

// GroupPrefix returns the first `depth` path segments of key, joined by '/'.
//
// For absolute Kubernetes-style keys the leading '/' consumes one split
// segment, so depth maps as:
//
//	/registry/pods/default/nginx
//	depth=1 -> /registry
//	depth=2 -> /registry/pods
//	depth=3 -> /registry/pods/default
//	depth=4 -> /registry/pods/default/nginx
//
// Keys shorter than depth segments are grouped under their full key.
func GroupPrefix(key string, depth int) string {
	if depth <= 0 {
		return "/"
	}
	parts := strings.Split(key, "/")
	n := depth + 1 // +1 for the leading empty segment of absolute keys
	if n > len(parts) {
		n = len(parts)
	}
	return strings.Join(parts[:n], "/")
}

// Summarize aggregates metas into prefix groups per cfg, returning the groups
// sorted by cfg.SortBy (descending) and truncated to cfg.Top.
//
// Revision bounds are NOT inclusion filters: every key contributes to Count,
// MaxVersion, LatestModRevision and size stats. Min/MaxCreateRevision and
// Min/MaxModRevision only gate the CreatedCount and ModifiedCount metrics, so
// `--sort=created-count --min-create-revision=<rev>` ranks prefixes by how
// many keys were created after <rev> without dropping older keys from Count.
func Summarize(metas []KeyMeta, cfg SummaryConfig) []GroupStats {
	if cfg.GroupDepth <= 0 {
		cfg.GroupDepth = 2
	}
	if cfg.Top <= 0 {
		cfg.Top = 20
	}
	if cfg.SortBy == "" {
		cfg.SortBy = "count"
	}

	groups := make(map[string]*GroupStats)
	order := make([]string, 0)

	for _, m := range metas {
		g := GroupPrefix(m.Key, cfg.GroupDepth)
		gs, ok := groups[g]
		if !ok {
			gs = &GroupStats{Group: g}
			groups[g] = gs
			order = append(order, g)
		}
		gs.Count++
		gs.MaxVersion = max64(gs.MaxVersion, m.Version)
		gs.LatestModRevision = max64(gs.LatestModRevision, m.ModRevision)
		if kv := m.KvSize(); kv >= 0 {
			gs.HasSize = true
			gs.TotalSize += kv
			gs.MaxSize = max64(gs.MaxSize, kv)
		}
		if inRange(m.CreateRevision, cfg.MinCreateRevision, cfg.MaxCreateRevision) {
			gs.CreatedCount++
		}
		if inRange(m.ModRevision, cfg.MinModRevision, cfg.MaxModRevision) {
			gs.ModifiedCount++
		}
		if m.RevCount != nil {
			gs.RevCount += int64(*m.RevCount)
		}
		if m.TombstoneCount != nil {
			gs.TombstoneCount += int64(*m.TombstoneCount)
		}
	}

	result := make([]GroupStats, 0, len(order))
	for _, g := range order {
		result = append(result, *groups[g])
	}

	sortGroups(result, cfg.SortBy)

	if cfg.Top < len(result) {
		// Build the synthetic "others" row: merge the dropped tail groups so the
		// user can see the aggregated count/size of the long tail at a glance
		// (template 3.2 / 3.3 "others (X prefixes)" row). avg_size / max_size /
		// max_version / max_mod_revision are left to the caller to render as "-".
		dropped := result[cfg.Top:]
		others := GroupStats{OthersCount: len(dropped)}
		if len(dropped) > 0 && dropped[0].HasSize {
			others.HasSize = true
		}
		for _, g := range dropped {
			others.Count += g.Count
			others.TotalSize += g.TotalSize
			others.CreatedCount += g.CreatedCount
			others.ModifiedCount += g.ModifiedCount
			others.RevCount += g.RevCount
			others.TombstoneCount += g.TombstoneCount
		}
		others.Group = fmt.Sprintf("others (%d)", len(dropped))
		result = append(result[:cfg.Top], others)
	}
	return result
}

// inRange reports whether v is within [min, max], treating 0 bounds as
// unbounded on that side.
func inRange(v, min, max int64) bool {
	if min > 0 && v < min {
		return false
	}
	if max > 0 && v > max {
		return false
	}
	return true
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func sortGroups(gs []GroupStats, by string) {
	less := func(i, j int) bool { return gs[i].Count > gs[j].Count }
	switch by {
	case "total-size":
		less = func(i, j int) bool { return gs[i].TotalSize > gs[j].TotalSize }
	case "avg-size":
		less = func(i, j int) bool { return gs[i].AvgSize() > gs[j].AvgSize() }
	case "max-size":
		less = func(i, j int) bool { return gs[i].MaxSize > gs[j].MaxSize }
	case "max-version":
		less = func(i, j int) bool { return gs[i].MaxVersion > gs[j].MaxVersion }
	case "latest-mod-revision":
		less = func(i, j int) bool { return gs[i].LatestModRevision > gs[j].LatestModRevision }
	case "created-count":
		less = func(i, j int) bool { return gs[i].CreatedCount > gs[j].CreatedCount }
	case "modified-count":
		less = func(i, j int) bool { return gs[i].ModifiedCount > gs[j].ModifiedCount }
	case "rev-count":
		less = func(i, j int) bool { return gs[i].RevCount > gs[j].RevCount }
	case "tombstone-count":
		less = func(i, j int) bool { return gs[i].TombstoneCount > gs[j].TombstoneCount }
	}
	sort.Slice(gs, less)
}
