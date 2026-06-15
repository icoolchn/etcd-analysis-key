package common

import (
	"encoding/json"
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// SizeOfKey returns the byte length of kv.Key.
func SizeOfKey(kv *mvccpb.KeyValue) int { return len(kv.Key) }

// MakeKVs creates a batch of KeyValue with the given key sizes.
func MakeKVs(sizes ...int) []*mvccpb.KeyValue {
	kvs := make([]*mvccpb.KeyValue, len(sizes))
	for i, s := range sizes {
		kvs[i] = &mvccpb.KeyValue{Key: make([]byte, s), Value: []byte("v")}
	}
	return kvs
}

// FeedAndRun creates a report, feeds data batches, and waits for completion.
func FeedAndRun(t *testing.T, bucketCount int, jsonMode bool, batches ...[]*mvccpb.KeyValue) core.Report {
	t.Helper()
	var opts []core.ReportOption
	if jsonMode {
		opts = append(opts, core.WithJSONMode())
	}
	r := core.NewReport(bucketCount, SizeOfKey, opts...)
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

// ReportJSON mirrors core.ReportJSON for external test parsing.
type ReportJSON struct {
	Summary     SummaryJSON    `json:"summary"`
	Histogram   []BucketJSON   `json:"histogram"`
	Percentiles map[string]int `json:"percentiles_bytes"`
}

type SummaryJSON struct {
	Count         int `json:"count"`
	TotalBytes    int `json:"total_bytes"`
	SmallestBytes int `json:"smallest_bytes"`
	LargestBytes  int `json:"largest_bytes"`
	AverageBytes  int `json:"average_bytes"`
}

type BucketJSON struct {
	BucketStart int `json:"bucket_start"`
	BucketEnd   int `json:"bucket_end"`
	Count       int `json:"count"`
}

// ParseReportJSON parses JSON output and asserts no error.
func ParseReportJSON(t *testing.T, output string) ReportJSON {
	t.Helper()
	var result ReportJSON
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("JSON parse error: %v\noutput: %s", err, output)
	}
	return result
}

// Benchmarker is an interface satisfied by both *testing.T and *testing.B.
type Benchmarker interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// FeedAndRunB is the benchmark variant of FeedAndRun.
func FeedAndRunB(b Benchmarker, bucketCount int, jsonMode bool, batches ...[]*mvccpb.KeyValue) core.Report {
	b.Helper()
	var opts []core.ReportOption
	if jsonMode {
		opts = append(opts, core.WithJSONMode())
	}
	r := core.NewReport(bucketCount, SizeOfKey, opts...)
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
