package unit

import (
	"testing"

	"github.com/SimFG/etcd-analysis/core"
)

func TestComputeGlobalStats_ScaleAndTimeRange(t *testing.T) {
	metas := []core.KeyMeta{
		{Key: "/registry/events/a", CreateRevision: 100, ModRevision: 200, Version: 1, Lease: 0, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/b", CreateRevision: 150, ModRevision: 250, Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/pods/c", CreateRevision: 300, ModRevision: 400, Version: 5000, Lease: 0, KeySizeBytes: 20, ValueSizeBytes: intPtr(80), KvSizeBytes: intPtr(100)},
	}
	g := core.ComputeGlobalStats(metas, "kv")
	if g.TotalKeys != 3 {
		t.Errorf("TotalKeys=%d want 3", g.TotalKeys)
	}
	// key sizes: 10+10+20=40; value: 50+50+80=180; kv: 60+60+100=220.
	if g.KeySize.Total != 40 || !g.KeySize.Available {
		t.Errorf("KeySize=%+v want Total 40 available", g.KeySize)
	}
	if g.ValueSize.Total != 180 || !g.ValueSize.Available {
		t.Errorf("ValueSize=%+v want Total 180 available", g.ValueSize)
	}
	if g.KvSize.Total != 220 || !g.KvSize.Available {
		t.Errorf("KvSize=%+v want Total 220 available", g.KvSize)
	}
	if g.LeaseZero != 2 {
		t.Errorf("LeaseZero=%d want 2", g.LeaseZero)
	}
	if g.MaxVersion != 5000 {
		t.Errorf("MaxVersion=%d want 5000", g.MaxVersion)
	}
	if g.CreateRevMin != 100 || g.CreateRevMax != 300 {
		t.Errorf("CreateRev min/max=%d/%d want 100/300", g.CreateRevMin, g.CreateRevMax)
	}
	if g.ModRevMin != 200 || g.ModRevMax != 400 {
		t.Errorf("ModRev min/max=%d/%d want 200/400", g.ModRevMin, g.ModRevMax)
	}
	if g.SizeBasis != "kv" {
		t.Errorf("SizeBasis=%q want kv", g.SizeBasis)
	}
}

func TestComputeGlobalStats_DiagnosisWriteHotspot(t *testing.T) {
	metas := []core.KeyMeta{
		{Key: "/registry/events/a", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/b", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/pods/c", Version: 5000, Lease: 99, KeySizeBytes: 20, ValueSizeBytes: intPtr(80), KvSizeBytes: intPtr(100)},
	}
	g := core.ComputeGlobalStats(metas, "kv")
	// version > 1000 -> WRITE HOTSPOT
	if g.Diagnosis.Version != "WRITE HOTSPOT" {
		t.Errorf("Version diagnosis=%q want WRITE HOTSPOT", g.Diagnosis.Version)
	}
	// top-1 (events) = 2/3 = 66.7% < 80%, top-3 = 100% >= 90% -> SKEWED
	if g.Diagnosis.Count != "SKEWED" {
		t.Errorf("Count diagnosis=%q want SKEWED", g.Diagnosis.Count)
	}
	// kv p99 = 100 bytes < 100KiB -> OK
	if g.Diagnosis.Size != "OK" {
		t.Errorf("Size diagnosis=%q want OK", g.Diagnosis.Size)
	}
	// lease=0 = 0 -> OK
	if g.Diagnosis.Lease != "OK" {
		t.Errorf("Lease diagnosis=%q want OK", g.Diagnosis.Lease)
	}
}

func TestComputeGlobalStats_DiagnosisConcentratedAndLargeValues(t *testing.T) {
	// 9 events under one prefix + 1 large value (200KiB). The large value drives
	// kv p99 >= 100KiB -> LARGE VALUES. p50 is not asserted (percentiles on small
	// inputs backfill high percentiles with the max).
	metas := []core.KeyMeta{
		{Key: "/registry/events/a", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/b", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/c", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/d", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/e", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/f", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/g", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/h", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/i", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/secrets/big", Version: 1, Lease: 0, KeySizeBytes: 30, ValueSizeBytes: intPtr(200 * 1024), KvSizeBytes: intPtr(200*1024 + 30)},
	}
	g := core.ComputeGlobalStats(metas, "kv")
	// top-1 = 9/10 = 90% >= 80% -> CONCENTRATED
	if g.Diagnosis.Count != "CONCENTRATED" {
		t.Errorf("Count diagnosis=%q want CONCENTRATED", g.Diagnosis.Count)
	}
	// kv p99 >= 100KiB -> LARGE VALUES (diagnosis uses kv, not --type)
	if g.Diagnosis.Size != "LARGE VALUES" {
		t.Errorf("Size diagnosis=%q want LARGE VALUES (kv p99=%d)", g.Diagnosis.Size, g.KvSize.P99)
	}
	// lease=0 = 1/10 = 10% < 20% -> OK
	if g.Diagnosis.Lease != "OK" {
		t.Errorf("Lease diagnosis=%q want OK", g.Diagnosis.Lease)
	}
}

func TestComputeGlobalStats_ThreeBasesAvailable(t *testing.T) {
	// One record: key=10B, value=50B, kv=60B. All three bases available.
	m := core.KeyMeta{
		Key: "/a", KeySizeBytes: 10,
		ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60),
	}
	g := core.ComputeGlobalStats([]core.KeyMeta{m}, "kv")
	for _, c := range []struct {
		name    string
		s       core.SizeStats
		total   int64
	}{
		{"key", g.KeySize, 10},
		{"value", g.ValueSize, 50},
		{"kv", g.KvSize, 60},
	} {
		if !c.s.Available {
			t.Errorf("%s: Available=false want true", c.name)
		}
		if c.s.Total != c.total {
			t.Errorf("%s: Total=%d want %d", c.name, c.s.Total, c.total)
		}
	}
}

func TestComputeGlobalStats_KeysOnlyValueAndKvUnavailable(t *testing.T) {
	// keys-only record: ValueSizeBytes / KvSizeBytes are nil.
	m := core.KeyMeta{Key: "/a", KeySizeBytes: 10}
	g := core.ComputeGlobalStats([]core.KeyMeta{m}, "value")
	// key size always available.
	if !g.KeySize.Available || g.KeySize.Total != 10 {
		t.Errorf("KeySize=%+v want available/10", g.KeySize)
	}
	// value and kv unavailable.
	if g.ValueSize.Available {
		t.Errorf("ValueSize=%+v want unavailable", g.ValueSize)
	}
	if g.KvSize.Available {
		t.Errorf("KvSize=%+v want unavailable", g.KvSize)
	}
	// diagnosis size uses kv -> unavailable -> OK with notice.
	if g.Diagnosis.Size != "OK" {
		t.Errorf("Diagnosis.Size=%q want OK (kv unavailable)", g.Diagnosis.Size)
	}
}

func TestComputeGlobalStats_HistoryRevisionsOffline(t *testing.T) {
	rc1, tc1 := 5, 2
	rc2, tc2 := 3, 0
	metas := []core.KeyMeta{
		{Key: "/a", KeySizeBytes: 1, RevCount: &rc1, TombstoneCount: &tc1},
		{Key: "/b", KeySizeBytes: 1, RevCount: &rc2, TombstoneCount: &tc2},
	}
	g := core.ComputeGlobalStats(metas, "kv")
	if !g.HasRevCount {
		t.Errorf("HasRevCount=false want true")
	}
	if g.HistoryRevisionsTotal != 8 {
		t.Errorf("HistoryRevisionsTotal=%d want 8", g.HistoryRevisionsTotal)
	}
	if g.TombstoneCountTotal != 2 {
		t.Errorf("TombstoneCountTotal=%d want 2", g.TombstoneCountTotal)
	}
}

func TestComputeGlobalStats_HistoryRevisionsOnlineUnavailable(t *testing.T) {
	// online records: RevCount / TombstoneCount are nil.
	m := core.KeyMeta{Key: "/a", KeySizeBytes: 1}
	g := core.ComputeGlobalStats([]core.KeyMeta{m}, "kv")
	if g.HasRevCount {
		t.Errorf("HasRevCount=true want false (online)")
	}
}
