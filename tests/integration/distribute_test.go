package integration

import (
	"strings"
	"testing"

	"github.com/SimFG/etcd-analysis/tests/common"
)

// --- Integration Tests: Full Pipeline ---

func TestDistribute_TextMode_FullPipeline(t *testing.T) {
	batch := common.MakeKVs(10, 20, 30, 40, 50)
	r := common.FeedAndRun(t, 5, false, batch)

	// Access the underlying report to call String()
	// Since Report interface doesn't expose String(), we use JSON() as proxy
	jsonOut := r.JSON()
	parsed := common.ParseReportJSON(t, jsonOut)

	if parsed.Summary.Count != 5 {
		t.Errorf("count = %d, want 5", parsed.Summary.Count)
	}
	if parsed.Summary.SmallestBytes != 10 {
		t.Errorf("smallest = %d, want 10", parsed.Summary.SmallestBytes)
	}
	if parsed.Summary.LargestBytes != 50 {
		t.Errorf("largest = %d, want 50", parsed.Summary.LargestBytes)
	}
}

func TestDistribute_JSONMode_FullPipeline(t *testing.T) {
	batch := common.MakeKVs(10, 20, 30)
	r := common.FeedAndRun(t, 3, true, batch)

	output := r.JSON()
	parsed := common.ParseReportJSON(t, output)

	if parsed.Summary.Count != 3 {
		t.Errorf("count = %d, want 3", parsed.Summary.Count)
	}
	if parsed.Summary.TotalBytes != 60 {
		t.Errorf("total = %d, want 60", parsed.Summary.TotalBytes)
	}
	if parsed.Summary.AverageBytes != 20 {
		t.Errorf("average = %d, want 20", parsed.Summary.AverageBytes)
	}

	// Verify histogram exists
	if len(parsed.Histogram) == 0 {
		t.Error("histogram should not be empty")
	}

	// Verify percentiles exist
	if len(parsed.Percentiles) == 0 {
		t.Error("percentiles should not be empty")
	}
}

func TestDistribute_JSONMode_EmptyData(t *testing.T) {
	r := common.FeedAndRun(t, 5, true) // no data

	output := r.JSON()
	parsed := common.ParseReportJSON(t, output)

	// Must NOT contain sentinel values
	if parsed.Summary.SmallestBytes == 2147483647 {
		t.Error("smallest_bytes must not be MaxInt32 sentinel")
	}
	if parsed.Summary.LargestBytes == -1 {
		t.Error("largest_bytes must not be -1 sentinel")
	}
	if parsed.Summary.Count != 0 {
		t.Errorf("count = %d, want 0", parsed.Summary.Count)
	}
	if parsed.Histogram == nil {
		t.Error("histogram should be empty array, not null")
	}
	if parsed.Percentiles == nil {
		t.Error("percentiles should be empty map, not null")
	}
}

func TestDistribute_MultipleBatches(t *testing.T) {
	batch1 := common.MakeKVs(10, 20)
	batch2 := common.MakeKVs(30, 40, 50)
	r := common.FeedAndRun(t, 5, true, batch1, batch2)

	parsed := common.ParseReportJSON(t, r.JSON())
	if parsed.Summary.Count != 5 {
		t.Errorf("count = %d, want 5", parsed.Summary.Count)
	}
	if parsed.Summary.TotalBytes != 150 {
		t.Errorf("total = %d, want 150", parsed.Summary.TotalBytes)
	}
}

// --- Contract Test: JSON Schema Validation ---

func TestDistribute_JSONOutput_SchemaValidation(t *testing.T) {
	batch := common.MakeKVs(10, 20, 30, 40, 50, 60, 70, 80, 90, 100)
	r := common.FeedAndRun(t, 5, true, batch)

	output := r.JSON()

	// Verify required top-level keys exist
	for _, key := range []string{`"summary"`, `"histogram"`, `"percentiles_bytes"`} {
		if !strings.Contains(output, key) {
			t.Errorf("JSON output should contain key %s", key)
		}
	}

	// Verify deprecated key is NOT present
	if strings.Contains(output, `"percentiles":`) {
		t.Error("JSON should not contain 'percentiles' key (should be 'percentiles_bytes')")
	}

	// Verify summary field names
	for _, field := range []string{`"count"`, `"total_bytes"`, `"smallest_bytes"`, `"largest_bytes"`, `"average_bytes"`} {
		if !strings.Contains(output, field) {
			t.Errorf("summary should contain field %s", field)
		}
	}

	// Verify percentile key format
	parsed := common.ParseReportJSON(t, output)
	for key := range parsed.Percentiles {
		if !strings.HasSuffix(key, "_bytes") {
			t.Errorf("percentile key %q should end with '_bytes'", key)
		}
	}

	// Verify histogram bucket structure
	for i, bucket := range parsed.Histogram {
		if bucket.BucketStart < 0 {
			t.Errorf("histogram[%d].bucket_start should be non-negative", i)
		}
		if bucket.Count < 0 {
			t.Errorf("histogram[%d].count should be non-negative", i)
		}
	}
}
