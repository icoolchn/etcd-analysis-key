package stress

import (
	"sync"
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/SimFG/etcd-analysis/tests/common"
)

// --- Stress Tests ---

func TestStress_10KKeys(t *testing.T) {
	// Generate 10000 keys with varying sizes
	sizes := make([]int, 10000)
	for i := range sizes {
		sizes[i] = (i % 500) + 1 // 1 to 500 bytes
	}

	batch := common.MakeKVs(sizes...)
	r := common.FeedAndRun(t, 10, true, batch)

	parsed := common.ParseReportJSON(t, r.JSON())

	if parsed.Summary.Count != 10000 {
		t.Errorf("count = %d, want 10000", parsed.Summary.Count)
	}
	if parsed.Summary.SmallestBytes != 1 {
		t.Errorf("smallest = %d, want 1", parsed.Summary.SmallestBytes)
	}
	if parsed.Summary.LargestBytes != 500 {
		t.Errorf("largest = %d, want 500", parsed.Summary.LargestBytes)
	}

	// Verify histogram is populated
	if len(parsed.Histogram) == 0 {
		t.Error("histogram should not be empty")
	}

	// Verify percentiles are populated
	if len(parsed.Percentiles) == 0 {
		t.Error("percentiles should not be empty")
	}
}

func TestStress_100Batches_Concurrent(t *testing.T) {
	r := core.NewReport(10, common.SizeOfKey, core.WithJSONMode())
	c := r.Results()
	donec := r.Run()

	// 10 goroutines each sending 10 batches of 100 KVs = 10000 total
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				c <- common.MakeKVs(makeHundredSizes()...)
			}
		}()
	}

	wg.Wait()
	close(c)
	<-donec

	parsed := common.ParseReportJSON(t, r.JSON())

	// 10 goroutines * 10 batches * 100 KVs = 10000
	if parsed.Summary.Count != 10000 {
		t.Errorf("count = %d, want 10000", parsed.Summary.Count)
	}
}

func TestStress_LargeValues(t *testing.T) {
	// Test with large key sizes (simulate big-key scenario)
	sizes := make([]int, 100)
	for i := range sizes {
		sizes[i] = (i + 1) * 1024 // 1KB to 100KB
	}

	batch := common.MakeKVs(sizes...)
	r := common.FeedAndRun(t, 10, true, batch)

	parsed := common.ParseReportJSON(t, r.JSON())

	if parsed.Summary.Count != 100 {
		t.Errorf("count = %d, want 100", parsed.Summary.Count)
	}
	if parsed.Summary.SmallestBytes != 1024 {
		t.Errorf("smallest = %d, want 1024", parsed.Summary.SmallestBytes)
	}
	if parsed.Summary.LargestBytes != 102400 {
		t.Errorf("largest = %d, want 102400", parsed.Summary.LargestBytes)
	}
}

// --- helpers ---

func makeHundredSizes() []int {
	sizes := make([]int, 100)
	for i := range sizes {
		sizes[i] = (i + 1) * 10
	}
	return sizes
}
