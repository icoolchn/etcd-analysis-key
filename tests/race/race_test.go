package race

import (
	"sync"
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/SimFG/etcd-analysis/tests/common"
)

// --- Race Detection Tests ---
// Run with: go test -race ./race/

func TestRace_DynamicOutput_4Goroutines_50Batches(t *testing.T) {
	r := core.NewReport(5, common.SizeOfKey)
	c := r.Results()
	donec := r.Run()

	// Start DynamicOutput (spawns goroutine calling String() every 100ms)
	r.DynamicOutput()

	// Feed data concurrently from 4 goroutines
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
}

func TestRace_JSON_4Goroutines_50Batches(t *testing.T) {
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

	// Verify JSON output is valid after concurrent writes
	parsed := common.ParseReportJSON(t, r.JSON())
	if parsed.Summary.Count != 1000 {
		t.Errorf("count = %d, want 1000", parsed.Summary.Count)
	}
}

func TestRace_String_ConcurrentAccess(t *testing.T) {
	r := core.NewReport(5, common.SizeOfKey)
	c := r.Results()
	donec := r.Run()

	r.DynamicOutput()

	// Concurrently feed data
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c <- common.MakeKVs(5, 15, 25, 35, 45)
			}
		}()
	}

	wg.Wait()
	close(c)
	<-donec
}
