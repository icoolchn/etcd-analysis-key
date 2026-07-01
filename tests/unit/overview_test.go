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
	g := core.ComputeGlobalStats(metas, 0)
	if g.TotalKeys != 3 {
		t.Errorf("TotalKeys=%d want 3", g.TotalKeys)
	}
	if g.TotalSize != 220 {
		t.Errorf("TotalSize=%d want 220", g.TotalSize)
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
	// offline: current revision falls back to max(mod_revision)
	if g.CurrentRevision != 400 || g.CurrentRevisionSource != "max(mod_revision)" {
		t.Errorf("CurrentRevision=%d (%s) want 400 (max(mod_revision))", g.CurrentRevision, g.CurrentRevisionSource)
	}
}

func TestComputeGlobalStats_DiagnosisWriteHotspot(t *testing.T) {
	metas := []core.KeyMeta{
		{Key: "/registry/events/a", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/events/b", Version: 1, Lease: 99, KeySizeBytes: 10, ValueSizeBytes: intPtr(50), KvSizeBytes: intPtr(60)},
		{Key: "/registry/pods/c", Version: 5000, Lease: 99, KeySizeBytes: 20, ValueSizeBytes: intPtr(80), KvSizeBytes: intPtr(100)},
	}
	g := core.ComputeGlobalStats(metas, 0)
	// version > 1000 -> WRITE HOTSPOT
	if g.Diagnosis.Version != "WRITE HOTSPOT" {
		t.Errorf("Version diagnosis=%q want WRITE HOTSPOT", g.Diagnosis.Version)
	}
	// top-1 (events) = 2/3 = 66.7% < 80%, top-3 = 100% >= 90% -> SKEWED
	if g.Diagnosis.Count != "SKEWED" {
		t.Errorf("Count diagnosis=%q want SKEWED", g.Diagnosis.Count)
	}
	// p99 = 100 bytes < 100KiB -> OK
	if g.Diagnosis.Size != "OK" {
		t.Errorf("Size diagnosis=%q want OK", g.Diagnosis.Size)
	}
	// lease=0 = 0 -> OK
	if g.Diagnosis.Lease != "OK" {
		t.Errorf("Lease diagnosis=%q want OK", g.Diagnosis.Lease)
	}
}

func TestComputeGlobalStats_DiagnosisConcentratedAndLargeValues(t *testing.T) {
	// 9 events under one prefix + 1 large value. The large value (200KiB)
	// drives p99 >= 100KiB -> LARGE VALUES. p50 is not asserted here because
	// percentiles() on small inputs backfills high percentiles with the max.
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
	g := core.ComputeGlobalStats(metas, 0)
	// top-1 = 9/10 = 90% >= 80% -> CONCENTRATED
	if g.Diagnosis.Count != "CONCENTRATED" {
		t.Errorf("Count diagnosis=%q want CONCENTRATED", g.Diagnosis.Count)
	}
	// p99 >= 100KiB -> LARGE VALUES
	if g.Diagnosis.Size != "LARGE VALUES" {
		t.Errorf("Size diagnosis=%q want LARGE VALUES (p99=%d)", g.Diagnosis.Size, g.SizeP99)
	}
	// lease=0 = 1/10 = 10% < 20% -> OK
	if g.Diagnosis.Lease != "OK" {
		t.Errorf("Lease diagnosis=%q want OK", g.Diagnosis.Lease)
	}
}

func TestComputeGlobalStats_OnlineCurrentRevision(t *testing.T) {
	metas := []core.KeyMeta{
		{Key: "/a", CreateRevision: 1, ModRevision: 100, Version: 1, Lease: 0, KeySizeBytes: 1, ValueSizeBytes: intPtr(1), KvSizeBytes: intPtr(2)},
	}
	// online: pass endpoint-status revision explicitly
	g := core.ComputeGlobalStats(metas, 99999)
	if g.CurrentRevision != 99999 || g.CurrentRevisionSource != "endpoint status" {
		t.Errorf("CurrentRevision=%d (%s) want 99999 (endpoint status)", g.CurrentRevision, g.CurrentRevisionSource)
	}
}
