package regression

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/SimFG/etcd-analysis/tests/common"
)

// --- Regression Tests for BUGFIX.md ---

// CR-Fix 1: histogramJSON sort correctness
func TestRegression_CR1_HistogramSortedBeforeBucketing(t *testing.T) {
	// Feed data in reverse order to verify sort.Ints is called before histogram
	batch := common.MakeKVs(40, 30, 10, 20, 40, 30, 10, 20)
	r := common.FeedAndRun(t, 3, true, batch)

	parsed := common.ParseReportJSON(t, r.JSON())

	// Histogram buckets must be in ascending order (proves sort was applied)
	for i := 1; i < len(parsed.Histogram); i++ {
		if parsed.Histogram[i].BucketStart < parsed.Histogram[i-1].BucketStart {
			t.Errorf("histogram buckets not sorted at index %d", i)
		}
	}

	// Non-zero histogram counts prove data was bucketed
	totalCount := 0
	for _, b := range parsed.Histogram {
		totalCount += b.Count
	}
	if totalCount == 0 {
		t.Error("histogram should have non-zero counts")
	}
}

// CR-Fix 2: JSON empty data guard
func TestRegression_CR2_EmptyDataNoSentinelValues(t *testing.T) {
	r := common.FeedAndRun(t, 5, true) // no data

	output := r.JSON()
	parsed := common.ParseReportJSON(t, output)

	if parsed.Summary.SmallestBytes == 2147483647 {
		t.Error("smallest_bytes must not be MaxInt32 (2147483647)")
	}
	if parsed.Summary.LargestBytes == -1 {
		t.Error("largest_bytes must not be -1")
	}
	if parsed.Summary.Count != 0 {
		t.Errorf("count = %d, want 0", parsed.Summary.Count)
	}
	if parsed.Histogram == nil {
		t.Error("histogram must not be null")
	}
	if parsed.Percentiles == nil {
		t.Error("percentiles must not be null")
	}
}

// CR-Fix 3: Functional options API
func TestRegression_CR3_FunctionalOptions(t *testing.T) {
	// NewReport without options should work
	r1 := core.NewReport(5, common.SizeOfKey)
	if r1 == nil {
		t.Fatal("NewReport without options returned nil")
	}

	// NewReport with WithJSONMode should work
	r2 := core.NewReport(5, common.SizeOfKey, core.WithJSONMode())
	if r2 == nil {
		t.Fatal("NewReport with WithJSONMode returned nil")
	}
}

// CR-Fix 7: Percentiles key format
func TestRegression_CR7_PercentilesBytesKey(t *testing.T) {
	batch := common.MakeKVs(10, 20, 30, 40, 50)
	r := common.FeedAndRun(t, 5, true, batch)

	output := r.JSON()

	// JSON tag must be "percentiles_bytes"
	if !strings.Contains(output, `"percentiles_bytes"`) {
		t.Error("JSON should contain key 'percentiles_bytes'")
	}
	if strings.Contains(output, `"percentiles":`) {
		t.Error("JSON should not contain old key 'percentiles'")
	}

	// Individual keys must end with _bytes
	parsed := common.ParseReportJSON(t, output)
	for key := range parsed.Percentiles {
		if !strings.HasSuffix(key, "_bytes") {
			t.Errorf("percentile key %q must end with '_bytes'", key)
		}
	}
}

// CR-Fix 9: String() data race
func TestRegression_CR9_NoRaceInString(t *testing.T) {
	r := core.NewReport(5, common.SizeOfKey)
	c := r.Results()
	donec := r.Run()

	r.DynamicOutput()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				c <- common.MakeKVs(10, 20, 30, 40, 50)
			}
		}()
	}

	wg.Wait()
	close(c)
	<-donec
	// No panic = pass (run with -race for full verification)
}

// CR-Fix 10: processResult field-level race
func TestRegression_CR10_NoRaceInProcessResult(t *testing.T) {
	r := core.NewReport(5, common.SizeOfKey, core.WithJSONMode())
	c := r.Results()
	donec := r.Run()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				c <- common.MakeKVs(10, 20, 30, 40, 50)
			}
		}()
	}

	wg.Wait()
	close(c)
	<-donec

	parsed := common.ParseReportJSON(t, r.JSON())
	if parsed.Summary.Count != 1000 {
		t.Errorf("count = %d, want 1000", parsed.Summary.Count)
	}
}

// CR-Fix 4: JSON mode should not sleep
func TestRegression_CR4_JSONModeNoSleep(t *testing.T) {
	start := time.Now()
	_ = common.FeedAndRun(t, 5, true, common.MakeKVs(10))
	elapsed := time.Since(start)

	// Text mode sleeps 100ms, JSON mode should complete much faster
	if elapsed > 80*time.Millisecond {
		t.Errorf("JSON mode took %v, expected < 80ms", elapsed)
	}
}
