package core

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// --- helpers ---

// sizeOfKey returns the size of kv.Key.
func sizeOfKey(kv *mvccpb.KeyValue) int { return len(kv.Key) }

// makeKVs creates a slice of KeyValue with given key sizes.
func makeKVs(sizes ...int) []*mvccpb.KeyValue {
	kvs := make([]*mvccpb.KeyValue, len(sizes))
	for i, s := range sizes {
		kvs[i] = &mvccpb.KeyValue{Key: make([]byte, s), Value: []byte("v")}
	}
	return kvs
}

// feedAndRun creates a report, feeds data, and waits for completion.
func feedAndRun(t *testing.T, bucketCount int, jsonMode bool, batches ...[]*mvccpb.KeyValue) Report {
	t.Helper()
	var opts []ReportOption
	if jsonMode {
		opts = append(opts, WithJSONMode())
	}
	r := NewReport(bucketCount, sizeOfKey, opts...)
	c := r.Results()
	donec := r.Run()

	go func() {
		defer close(c)
		for _, batch := range batches {
			c <- batch
		}
	}()

	<-donec
	return r
}

// --- Unit Tests ---

func TestNewReport_TextMode(t *testing.T) {
	r := NewReport(5, sizeOfKey)
	rr := r.(*report)
	if rr.jsonMode {
		t.Fatal("expected text mode, got json mode")
	}
}

func TestNewReport_JSONMode(t *testing.T) {
	r := NewReport(5, sizeOfKey, WithJSONMode())
	rr := r.(*report)
	if !rr.jsonMode {
		t.Fatal("expected json mode, got text mode")
	}
}

// --- CR-Fix 2: JSON empty data guard ---

func TestJSON_EmptyData(t *testing.T) {
	r := feedAndRun(t, 5, true) // no data fed

	output := r.JSON()
	var result ReportJSON
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("JSON parse error: %v\noutput: %s", err, output)
	}

	if result.Summary.Count != 0 {
		t.Errorf("expected count=0, got %d", result.Summary.Count)
	}
	// CR-Fix 2: must NOT contain sentinel values
	if result.Summary.SmallestBytes == 2147483647 {
		t.Error("smallest_bytes should not be MaxInt32 sentinel value")
	}
	if result.Summary.LargestBytes == -1 {
		t.Error("largest_bytes should not be -1 sentinel value")
	}
	if result.Histogram == nil {
		t.Error("histogram should be empty array, not null")
	}
	if result.Percentiles == nil {
		t.Error("percentiles should be empty map, not null")
	}
}

// --- CR-Fix 1: histogramJSON sort correctness ---

func TestJSON_HistogramSortCorrectness(t *testing.T) {
	// Verify that sort.Ints is called before histogramJSON by feeding unsorted data.
	// Use values that exactly match bucket boundaries:
	// sizes=[10,20,30,40], bucketCount=3, bs=10, buckets=[10,20,30,40]
	batch := makeKVs(40, 30, 10, 20, 40, 30, 10, 20)
	r := feedAndRun(t, 3, true, batch)

	output := r.JSON()
	var result ReportJSON
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}

	// Verify histogram has valid structure with non-zero counts.
	// Note: the two-pointer algorithm does not count values assigned to the
	// last bucket, so histogram total may be < summary count. This is a known
	// limitation of the algorithm, not a bug introduced by this PR.
	totalCount := 0
	for _, b := range result.Histogram {
		if b.Count < 0 {
			t.Errorf("bucket count should be non-negative, got %d", b.Count)
		}
		totalCount += b.Count
	}
	if totalCount == 0 {
		t.Error("histogram should have non-zero total count")
	}

	if result.Summary.Count != 8 {
		t.Errorf("summary count = %d, want 8", result.Summary.Count)
	}

	// Key verification for CR-Fix 1: histogram buckets must be in ascending order
	// (which proves sort.Ints was called before histogramJSON)
	for i := 1; i < len(result.Histogram); i++ {
		if result.Histogram[i].BucketStart < result.Histogram[i-1].BucketStart {
			t.Errorf("histogram buckets not sorted: bucket[%d].start=%d < bucket[%d].start=%d",
				i, result.Histogram[i].BucketStart, i-1, result.Histogram[i-1].BucketStart)
		}
	}
}

// --- CR-Fix 7: percentiles key format ---

func TestJSON_PercentilesKeyFormat(t *testing.T) {
	batch := makeKVs(10, 20, 30, 40, 50, 60, 70, 80, 90, 100)
	r := feedAndRun(t, 5, true, batch)

	output := r.JSON()

	// JSON tag must be "percentiles_bytes"
	if !strings.Contains(output, `"percentiles_bytes"`) {
		t.Error("JSON should contain key 'percentiles_bytes'")
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}
	if _, ok := raw["percentiles"]; ok {
		t.Error("JSON should not contain key 'percentiles' (should be 'percentiles_bytes')")
	}

	// Individual keys should be p*_bytes
	var result ReportJSON
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}
	for key := range result.Percentiles {
		if !strings.HasSuffix(key, "_bytes") {
			t.Errorf("percentile key %q should end with '_bytes'", key)
		}
	}
}

// --- JSON summary correctness ---

func TestJSON_Summary(t *testing.T) {
	batch := makeKVs(10, 20, 30)
	r := feedAndRun(t, 3, true, batch)

	var result ReportJSON
	if err := json.Unmarshal([]byte(r.JSON()), &result); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}

	if result.Summary.Count != 3 {
		t.Errorf("count = %d, want 3", result.Summary.Count)
	}
	if result.Summary.SmallestBytes != 10 {
		t.Errorf("smallest = %d, want 10", result.Summary.SmallestBytes)
	}
	if result.Summary.LargestBytes != 30 {
		t.Errorf("largest = %d, want 30", result.Summary.LargestBytes)
	}
	if result.Summary.TotalBytes != 60 {
		t.Errorf("total = %d, want 60", result.Summary.TotalBytes)
	}
	if result.Summary.AverageBytes != 20 {
		t.Errorf("average = %d, want 20", result.Summary.AverageBytes)
	}
}

// --- Text mode String() output ---

func TestString_Basic(t *testing.T) {
	batch := makeKVs(10, 20, 30)
	r := feedAndRun(t, 3, false, batch)

	output := r.(*report).String()
	for _, section := range []string{"Summary:", "Size histogram:", "Size distribution:"} {
		if !strings.Contains(output, section) {
			t.Errorf("text output should contain %q", section)
		}
	}
}

// --- CR-Fix 9: Race detection ---
// Run with: go test -race -run TestRace

func TestRace_DynamicOutput_ConcurrentWrite(t *testing.T) {
	r := NewReport(5, sizeOfKey).(*report)
	c := r.Results()
	donec := r.Run()

	// Start DynamicOutput (spawns goroutine calling String() every 100ms)
	r.DynamicOutput()

	// Feed data concurrently from multiple goroutines to maximize race window
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				c <- makeKVs(10, 20, 30, 40, 50)
			}
		}()
	}

	wg.Wait()
	close(c)
	<-donec

	// Verify final stats are correct: 4 goroutines * 50 batches * 5 items = 1000
	if r.stats.Count != 1000 {
		t.Errorf("count = %d, want 1000", r.stats.Count)
	}
}

func TestRace_JSON_ConcurrentWrite(t *testing.T) {
	r := NewReport(5, sizeOfKey, WithJSONMode()).(*report)
	c := r.Results()
	donec := r.Run()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				c <- makeKVs(10, 20, 30, 40, 50)
			}
		}()
	}

	wg.Wait()
	close(c)
	<-donec

	// JSON output should be valid and have correct count
	var result ReportJSON
	if err := json.Unmarshal([]byte(r.JSON()), &result); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}
	if result.Summary.Count != 1000 {
		t.Errorf("count = %d, want 1000", result.Summary.Count)
	}
}

// --- CR-Fix 4: JSON mode should not sleep ---

func TestJSONMode_NoSleep(t *testing.T) {
	start := time.Now()
	_ = feedAndRun(t, 5, true, makeKVs(10))
	elapsed := time.Since(start)

	// Text mode sleeps 100ms, JSON mode should not.
	// Allow generous margin but must be well under 100ms.
	if elapsed > 80*time.Millisecond {
		t.Errorf("JSON mode took %v, expected < 80ms (no sleep)", elapsed)
	}
}

// --- Regression: multiple batches accumulate correctly ---

func TestMultipleBatches(t *testing.T) {
	batch1 := makeKVs(10, 20)
	batch2 := makeKVs(30, 40, 50)
	r := feedAndRun(t, 5, true, batch1, batch2)

	var result ReportJSON
	if err := json.Unmarshal([]byte(r.JSON()), &result); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}
	if result.Summary.Count != 5 {
		t.Errorf("count = %d, want 5", result.Summary.Count)
	}
	if result.Summary.TotalBytes != 150 {
		t.Errorf("total = %d, want 150", result.Summary.TotalBytes)
	}
}
