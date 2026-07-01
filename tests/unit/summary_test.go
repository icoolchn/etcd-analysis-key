package unit

import (
	"testing"

	"github.com/SimFG/etcd-analysis/core"
)

func meta(key string, create, mod, version int64, kvSize int) core.KeyMeta {
	m := core.KeyMeta{
		Key:            key,
		KeySizeBytes:   len(key),
		CreateRevision: create,
		ModRevision:    mod,
		Version:        version,
	}
	if kvSize >= 0 {
		vs := kvSize - len(key)
		ks := kvSize
		m.ValueSizeBytes = &vs
		m.KvSizeBytes = &ks
	}
	return m
}

func TestGroupPrefix_Depth(t *testing.T) {
	cases := []struct {
		key   string
		depth int
		want  string
	}{
		{"/registry/pods/default/nginx", 1, "/registry"},
		{"/registry/pods/default/nginx", 2, "/registry/pods"},
		{"/registry/pods/default/nginx", 3, "/registry/pods/default"},
		{"/registry/pods/default/nginx", 4, "/registry/pods/default/nginx"},
		{"/registry/pods/default/nginx", 10, "/registry/pods/default/nginx"}, // deeper than key
		{"qaenv", 2, "qaenv"},
		{"/qaenv", 1, "/qaenv"},
	}
	for _, c := range cases {
		got := core.GroupPrefix(c.key, c.depth)
		if got != c.want {
			t.Errorf("GroupPrefix(%q, %d) = %q, want %q", c.key, c.depth, got, c.want)
		}
	}
}

func TestSummarize_GroupingAndCount(t *testing.T) {
	metas := []core.KeyMeta{
		meta("/registry/pods/default/a", 1, 10, 1, 100),
		meta("/registry/pods/default/b", 2, 20, 5, 200),
		meta("/registry/pods/kube-system/c", 3, 30, 1, 50),
		meta("/registry/services/default/s", 4, 40, 1, 300),
	}
	groups := core.Summarize(metas, core.SummaryConfig{GroupDepth: 2, Top: 10, SortBy: "count"})
	// depth=2 -> /registry/pods (3), /registry/services (1)
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].Group != "/registry/pods" || groups[0].Count != 3 {
		t.Errorf("top group = %+v, want /registry/pods count 3", groups[0])
	}
	// /registry/pods total size = 100+200+50 = 350, max=200, max-version=5, latest-mod=30
	pods := groups[0]
	if pods.TotalSize != 350 || pods.MaxSize != 200 || pods.MaxVersion != 5 || pods.LatestModRevision != 30 {
		t.Errorf("pods stats wrong: %+v", pods)
	}
	if pods.AvgSize() != 350/3 {
		t.Errorf("avg size = %d, want %d", pods.AvgSize(), 350/3)
	}
}

func TestSummarize_SortByTotalSize(t *testing.T) {
	metas := []core.KeyMeta{
		meta("/a/x", 1, 1, 1, 100),
		meta("/b/x", 1, 1, 1, 500),
		meta("/b/y", 1, 1, 1, 500),
	}
	// depth=1 -> /a total 100, /b total 1000
	groups := core.Summarize(metas, core.SummaryConfig{GroupDepth: 1, Top: 10, SortBy: "total-size"})
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].Group != "/b" || groups[0].TotalSize != 1000 {
		t.Errorf("sort by total-size wrong: %+v", groups[0])
	}
}

func TestSummarize_TopTruncation(t *testing.T) {
	metas := make([]core.KeyMeta, 0, 10)
	for i := 0; i < 10; i++ {
		metas = append(metas, meta("/p"+string(rune('a'+i))+"/x", 1, 1, 1, 10))
	}
	// 10 distinct groups; Top=3 -> 3 shown + 1 synthetic "others" row = 4.
	groups := core.Summarize(metas, core.SummaryConfig{GroupDepth: 1, Top: 3, SortBy: "count"})
	if len(groups) != 4 {
		t.Fatalf("expected 4 rows (3 top + others), got %d", len(groups))
	}
	others := groups[3]
	if !others.IsOthers() {
		t.Fatalf("expected 4th row to be others, got %+v", others)
	}
	if others.OthersCount != 7 {
		t.Errorf("expected others to merge 7 dropped groups, got %d", others.OthersCount)
	}
	if others.Count != 7 {
		t.Errorf("expected others count=7, got %d", others.Count)
	}
}

func TestSummarize_TopNoTruncationNoOthers(t *testing.T) {
	metas := []core.KeyMeta{
		meta("/a/x", 1, 1, 1, 10),
		meta("/b/x", 1, 1, 1, 10),
	}
	// Top=10 but only 2 groups -> no truncation, no others row.
	groups := core.Summarize(metas, core.SummaryConfig{GroupDepth: 1, Top: 10, SortBy: "count"})
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	for _, g := range groups {
		if g.IsOthers() {
			t.Errorf("unexpected others row when no truncation: %+v", g)
		}
	}
}

func TestSummarize_RevisionMetrics(t *testing.T) {
	metas := []core.KeyMeta{
		meta("/a/x", 100, 500, 1, 10),  // created>=100, mod>=500
		meta("/a/y", 50, 200, 1, 10),   // created<100, mod<500
		meta("/a/z", 150, 600, 1, 10),  // created>=100, mod>=500
	}
	// Revision bounds gate created-count/modified-count but NOT count.
	groups := core.Summarize(metas, core.SummaryConfig{
		GroupDepth:        1,
		Top:               10,
		SortBy:            "count",
		MinCreateRevision: 100,
		MinModRevision:    500,
	})
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	g := groups[0]
	if g.Count != 3 {
		t.Errorf("Count = %d, want 3 (revision bounds must not filter count)", g.Count)
	}
	if g.CreatedCount != 2 {
		t.Errorf("CreatedCount = %d, want 2", g.CreatedCount)
	}
	if g.ModifiedCount != 2 {
		t.Errorf("ModifiedCount = %d, want 2", g.ModifiedCount)
	}
}

func TestSummarize_KeysOnlyHasNoSize(t *testing.T) {
	// kvSize=-1 means keys-only: sizes stay zero, HasSize false.
	metas := []core.KeyMeta{
		meta("/a/x", 1, 1, 1, -1),
		meta("/a/y", 1, 1, 1, -1),
	}
	groups := core.Summarize(metas, core.SummaryConfig{GroupDepth: 1, Top: 10, SortBy: "count"})
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].HasSize {
		t.Error("keys-only group should not have size stats")
	}
	if groups[0].TotalSize != 0 || groups[0].MaxSize != 0 {
		t.Errorf("keys-only size stats should be 0: %+v", groups[0])
	}
}

func TestValidSortKey(t *testing.T) {
	for _, k := range []string{"count", "total-size", "avg-size", "max-size", "max-version", "latest-mod-revision", "created-count", "modified-count", "rev-count", "tombstone-count"} {
		if !core.ValidSortKey(k) {
			t.Errorf("%q should be valid", k)
		}
	}
	if core.ValidSortKey("nope") {
		t.Error("\"nope\" should be invalid")
	}
}

func metaWithRevCount(key string, create, mod, version int64, kvSize, revCount, tombstoneCount int) core.KeyMeta {
	m := meta(key, create, mod, version, kvSize)
	m.RevCount = &revCount
	m.TombstoneCount = &tombstoneCount
	return m
}

func TestSummarize_SortByRevCount(t *testing.T) {
	metas := []core.KeyMeta{
		metaWithRevCount("/a/x", 1, 1, 1, 10, 5, 1),
		metaWithRevCount("/a/y", 1, 1, 1, 10, 10, 2),
		metaWithRevCount("/b/x", 1, 1, 1, 10, 100, 0),
	}
	groups := core.Summarize(metas, core.SummaryConfig{GroupDepth: 1, Top: 10, SortBy: "rev-count"})
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].Group != "/b" {
		t.Errorf("sort by rev-count: first = %q, want /b (100)", groups[0].Group)
	}
	if groups[0].RevCount != 100 {
		t.Errorf("/b RevCount = %d, want 100", groups[0].RevCount)
	}
	if groups[1].RevCount != 15 {
		t.Errorf("/a RevCount = %d, want 15", groups[1].RevCount)
	}
}

func TestSummarize_SortByTombstoneCount(t *testing.T) {
	metas := []core.KeyMeta{
		metaWithRevCount("/a/x", 1, 1, 1, 10, 5, 10),
		metaWithRevCount("/b/x", 1, 1, 1, 10, 100, 1),
	}
	groups := core.Summarize(metas, core.SummaryConfig{GroupDepth: 1, Top: 10, SortBy: "tombstone-count"})
	if groups[0].Group != "/a" {
		t.Errorf("sort by tombstone-count: first = %q, want /a (10)", groups[0].Group)
	}
	if groups[0].TombstoneCount != 10 {
		t.Errorf("/a TombstoneCount = %d, want 10", groups[0].TombstoneCount)
	}
}

func TestSummarize_RevCountAggregation(t *testing.T) {
	metas := []core.KeyMeta{
		metaWithRevCount("/g/a", 1, 1, 1, 10, 3, 1),
		metaWithRevCount("/g/b", 1, 1, 1, 10, 7, 2),
	}
	groups := core.Summarize(metas, core.SummaryConfig{GroupDepth: 1, Top: 10, SortBy: "count"})
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].RevCount != 10 {
		t.Errorf("RevCount = %d, want 10 (3+7)", groups[0].RevCount)
	}
	if groups[0].TombstoneCount != 3 {
		t.Errorf("TombstoneCount = %d, want 3 (1+2)", groups[0].TombstoneCount)
	}
}

// TestSummarize_WithFilter chains the offline summary path: JSONL-style metas
// → FilterMetas → Summarize, mirroring how `summary --input --filter` behaves.
func TestSummarize_WithFilter(t *testing.T) {
	metas := []core.KeyMeta{
		meta("/a/small", 1, 1, 1, 10),   // kv=10
		meta("/a/big", 1, 1, 1, 1000),   // kv=1000
		meta("/b/big", 1, 1, 1, 2000),   // kv=2000
	}
	// Keep only kv >= 1000: drops /a/small, keeps /a/big and /b/big.
	filtered := core.FilterMetas(metas, core.FilterConfig{Attribute: "kv", Min: 1000})
	if len(filtered) != 2 {
		t.Fatalf("expected 2 after filter, got %d", len(filtered))
	}
	groups := core.Summarize(filtered, core.SummaryConfig{GroupDepth: 1, Top: 10, SortBy: "total-size"})
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	// /b (2000) > /a (1000) by total-size.
	if groups[0].Group != "/b" || groups[0].TotalSize != 2000 {
		t.Errorf("top group wrong: %+v", groups[0])
	}
	if groups[1].Group != "/a" || groups[1].TotalSize != 1000 {
		t.Errorf("second group wrong: %+v", groups[1])
	}
}
