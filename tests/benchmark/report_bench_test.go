package benchmark

import (
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/SimFG/etcd-analysis/tests/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// --- Benchmark: processResult throughput ---

func BenchmarkProcessResult_SingleBatch(b *testing.B) {
	batch := common.MakeKVs(generateSizes(1000)...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := core.NewReport(10, common.SizeOfKey, core.WithJSONMode())
		c := r.Results()
		donec := r.Run()
		go func() {
			defer close(c)
			c <- batch
		}()
		<-donec
	}
}

func BenchmarkProcessResult_MultipleBatches(b *testing.B) {
	batches := make([][]*mvccpb.KeyValue, 10)
	for i := range batches {
		batches[i] = common.MakeKVs(generateSizes(100)...)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := core.NewReport(10, common.SizeOfKey, core.WithJSONMode())
		c := r.Results()
		donec := r.Run()
		go func() {
			defer close(c)
			for _, batch := range batches {
				c <- batch
			}
		}()
		<-donec
	}
}

// --- Benchmark: JSON output ---

func BenchmarkJSON_Output(b *testing.B) {
	batch := common.MakeKVs(generateSizes(1000)...)
	r := common.FeedAndRunB(b, 10, true, batch)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.JSON()
	}
}

func BenchmarkJSON_Output_SmallData(b *testing.B) {
	batch := common.MakeKVs(10, 20, 30, 40, 50)
	r := common.FeedAndRunB(b, 5, true, batch)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.JSON()
	}
}

// --- helpers ---

func generateSizes(n int) []int {
	sizes := make([]int, n)
	for i := range sizes {
		sizes[i] = (i%100 + 1) * 10 // 10, 20, ..., 1000
	}
	return sizes
}
