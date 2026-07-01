package unit

import (
	"bytes"
	"os"
	"testing"

	"github.com/SimFG/etcd-analysis/core"
)

func TestWalOp_JSONLRoundTrip(t *testing.T) {
	ops := []core.WalOp{
		{RaftIndex: 10, RaftTerm: 1, OpType: "Put", Key: "/a/b", ValueSizeBytes: 42, EntryType: "IRRPut"},
		{RaftIndex: 11, RaftTerm: 1, OpType: "DeleteRange", Key: "/a/c", EntryType: "IRRDeleteRange"},
		{RaftIndex: 12, RaftTerm: 2, OpType: "Txn", IsTxn: true, Key: "/tx", EntryType: "IRRTxn"},
		{RaftIndex: 13, RaftTerm: 2, OpType: "Compaction", EntryType: "IRRCompaction"},
	}

	var buf bytes.Buffer
	if err := core.WriteWalJSONL(ops, &buf); err != nil {
		t.Fatal(err)
	}

	tmpFile := t.TempDir() + "/wal.jsonl"
	if err := os.WriteFile(tmpFile, buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}

	got, err := core.ReadWalJSONL(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != len(ops) {
		t.Fatalf("len = %d, want %d", len(got), len(ops))
	}

	for i := range ops {
		if got[i].RaftIndex != ops[i].RaftIndex {
			t.Errorf("[%d] RaftIndex = %d, want %d", i, got[i].RaftIndex, ops[i].RaftIndex)
		}
		if got[i].RaftTerm != ops[i].RaftTerm {
			t.Errorf("[%d] RaftTerm = %d, want %d", i, got[i].RaftTerm, ops[i].RaftTerm)
		}
		if got[i].OpType != ops[i].OpType {
			t.Errorf("[%d] OpType = %q, want %q", i, got[i].OpType, ops[i].OpType)
		}
		if got[i].Key != ops[i].Key {
			t.Errorf("[%d] Key = %q, want %q", i, got[i].Key, ops[i].Key)
		}
		if got[i].ValueSizeBytes != ops[i].ValueSizeBytes {
			t.Errorf("[%d] ValueSizeBytes = %d, want %d", i, got[i].ValueSizeBytes, ops[i].ValueSizeBytes)
		}
		if got[i].IsTxn != ops[i].IsTxn {
			t.Errorf("[%d] IsTxn = %v, want %v", i, got[i].IsTxn, ops[i].IsTxn)
		}
		if got[i].EntryType != ops[i].EntryType {
			t.Errorf("[%d] EntryType = %q, want %q", i, got[i].EntryType, ops[i].EntryType)
		}
	}
}

func TestWalOp_JSONLOmitsEmptyFields(t *testing.T) {
	ops := []core.WalOp{
		{RaftIndex: 1, RaftTerm: 1, OpType: "Compaction", EntryType: "IRRCompaction"},
	}
	var buf bytes.Buffer
	if err := core.WriteWalJSONL(ops, &buf); err != nil {
		t.Fatal(err)
	}
	line := buf.String()
	if bytes.Contains([]byte(line), []byte(`"key"`)) {
		t.Errorf("empty Key should be omitted: %s", line)
	}
	if bytes.Contains([]byte(line), []byte(`"value_size_bytes"`)) {
		t.Errorf("zero ValueSizeBytes should be omitted: %s", line)
	}
	if bytes.Contains([]byte(line), []byte(`"is_txn"`)) {
		t.Errorf("false IsTxn should be omitted: %s", line)
	}
}

func TestAggregateWalOps(t *testing.T) {
	ops := []core.WalOp{
		{OpType: "Put", Key: "/a", ValueSizeBytes: 10},
		{OpType: "Put", Key: "/a", ValueSizeBytes: 20},
		{OpType: "DeleteRange", Key: "/a"},
		{OpType: "Put", Key: "/b", ValueSizeBytes: 5},
		{OpType: "Compaction"},
	}

	stats := core.AggregateWalOps(ops)
	if len(stats) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(stats))
	}

	var a, b *core.WalKeyStats
	for i := range stats {
		switch stats[i].Key {
		case "/a":
			a = &stats[i]
		case "/b":
			b = &stats[i]
		}
	}
	if a == nil || b == nil {
		t.Fatal("missing stats for /a or /b")
	}
	if a.PutCount != 2 || a.DeleteCount != 1 || a.TotalValueSizeBytes != 30 {
		t.Errorf("/a = put:%d del:%d totalVal:%d, want put:2 del:1 totalVal:30",
			a.PutCount, a.DeleteCount, a.TotalValueSizeBytes)
	}
	if b.PutCount != 1 || b.DeleteCount != 0 || b.TotalValueSizeBytes != 5 {
		t.Errorf("/b = put:%d del:%d totalVal:%d, want put:1 del:0 totalVal:5",
			b.PutCount, b.DeleteCount, b.TotalValueSizeBytes)
	}
}

func TestSortWalKeyStats(t *testing.T) {
	stats := []core.WalKeyStats{
		{Key: "/low", PutCount: 1, DeleteCount: 10},
		{Key: "/high", PutCount: 100, DeleteCount: 1},
	}

	core.SortWalKeyStats(stats, "put-count")
	if stats[0].Key != "/high" {
		t.Errorf("sort by put-count: first = %q, want /high", stats[0].Key)
	}

	core.SortWalKeyStats(stats, "delete-count")
	if stats[0].Key != "/low" {
		t.Errorf("sort by delete-count: first = %q, want /low", stats[0].Key)
	}

	core.SortWalKeyStats(stats, "total-ops")
	if stats[0].Key != "/high" {
		t.Errorf("sort by total-ops: first = %q, want /high (101 > 11)", stats[0].Key)
	}
}

func TestReadWalJSONL_InvalidFile(t *testing.T) {
	_, err := core.ReadWalJSONL("/nonexistent/file.jsonl")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestReadWalJSONL_InvalidJSON(t *testing.T) {
	tmpFile := t.TempDir() + "/bad.jsonl"
	if err := os.WriteFile(tmpFile, []byte("not json\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := core.ReadWalJSONL(tmpFile)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
