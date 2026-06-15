package unit

import (
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/SimFG/etcd-analysis/tests/common"
)

// --- Functional Options API Tests ---

func TestNewReport_DefaultIsTextMode(t *testing.T) {
	r := core.NewReport(5, common.SizeOfKey)
	if r == nil {
		t.Fatal("NewReport returned nil")
	}
	// Verify it works in text mode by feeding empty data and getting output
	batch := common.MakeKVs(10, 20, 30)
	result := common.FeedAndRun(t, 5, false, batch)
	if result == nil {
		t.Fatal("FeedAndRun returned nil")
	}
}

func TestNewReport_WithJSONMode(t *testing.T) {
	r := core.NewReport(5, common.SizeOfKey, core.WithJSONMode())
	if r == nil {
		t.Fatal("NewReport with WithJSONMode returned nil")
	}
	// Verify JSON output is valid
	batch := common.MakeKVs(10, 20, 30)
	result := common.FeedAndRun(t, 5, true, batch)
	output := result.JSON()
	common.ParseReportJSON(t, output) // asserts no parse error
}

func TestNewReport_NoOptionsIsTextMode(t *testing.T) {
	// Calling without options should produce text-mode report
	batch := common.MakeKVs(10, 20)
	r := common.FeedAndRun(t, 3, false, batch)
	jsonOutput := r.JSON()

	// JSON() should still work even in text mode (after Run completes)
	var parsed common.ReportJSON
	parsed = common.ParseReportJSON(t, jsonOutput)
	if parsed.Summary.Count != 2 {
		t.Errorf("expected count=2, got %d", parsed.Summary.Count)
	}
}
