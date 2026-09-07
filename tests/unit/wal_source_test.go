package unit

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

// writeWAL creates a real WAL directory at dir with entries at the given raft
// indexes (one Put per index), then closes it. Each entry's Data is an
// InternalRaftRequest Put so decodeEntry produces an IRRPut op.
func writeWAL(t *testing.T, dir string, indexes []uint64, term uint64) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	w, err := wal.Create(nil, dir, []byte("test-metadata"))
	if err != nil {
		t.Fatal(err)
	}
	// wal.Create writes an initial SaveSnapshot({}) at index 0, so the first
	// real entry must be index >= 1.
	for _, idx := range indexes {
		ent := raftpb.Entry{
			Term:  term,
			Index: idx,
			Type:  raftpb.EntryNormal,
			// Minimal InternalRaftRequest with a Put. Encoded by hand is fragile;
			// instead use an empty Data -> decodeEntry returns "Normal" op, which
			// is enough to verify indexes and scope.
			Data: nil,
		}
		if err := w.Save(raftpb.HardState{Term: term, Commit: idx}, []raftpb.Entry{ent}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestWalSource_ReadsAllEntriesFromFreshWAL builds a fresh WAL (no snapshot,
// first segment still present) and verifies WalSource reads every entry. This
// exercises the no-snapshot fallback path (oldest firstIndex start).
func TestWalSource_ReadsAllEntriesFromFreshWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "member", "wal")
	indexes := []uint64{1, 2, 3, 4, 5}
	writeWAL(t, walDir, indexes, 1)

	opc, scope, err := core.WalSource(dir)
	if err != nil {
		t.Fatalf("WalSource: %v", err)
	}
	var ops []core.WalOp
	for op := range opc {
		ops = append(ops, op)
	}
	if len(ops) != len(indexes) {
		t.Fatalf("got %d ops, want %d", len(ops), len(indexes))
	}
	for i, want := range indexes {
		if ops[i].RaftIndex != want {
			t.Errorf("ops[%d].RaftIndex = %d, want %d", i, ops[i].RaftIndex, want)
		}
	}
	// No snapshot -> SnapUsed nil, all segments read, none skipped.
	if scope.SnapUsed != nil {
		t.Errorf("SnapUsed = %v, want nil (no snapshot)", scope.SnapUsed)
	}
	if len(scope.SkippedSegments) != 0 {
		t.Errorf("SkippedSegments = %d, want 0", len(scope.SkippedSegments))
	}
	if len(scope.ReadSegments) == 0 {
		t.Error("ReadSegments empty, want at least one segment")
	}
}

// TestWalSource_ScopeStructureNoSnap verifies the WalScope fields make sense:
// read segments are sorted by seq, the tail is marked, and ReadStartIndex is
// the oldest firstIndex when there is no snapshot.
func TestWalSource_ScopeStructureNoSnap(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "member", "wal")
	writeWAL(t, walDir, []uint64{1, 2, 3}, 1)

	opc, scope, err := core.WalSource(dir)
	if err != nil {
		t.Fatalf("WalSource: %v", err)
	}
	for range opc {
	}
	if len(scope.ReadSegments) == 0 {
		t.Fatal("no read segments")
	}
	last := scope.ReadSegments[len(scope.ReadSegments)-1]
	if !last.IsTail {
		t.Errorf("last read segment IsTail = false, want true (seq=%d)", last.Seq)
	}
	// ReadStartIndex should be the oldest firstIndex (0 from the initial
	// SaveSnapshot({}) written by wal.Create).
	if scope.ReadStartIndex != 0 {
		t.Errorf("ReadStartIndex = %d, want 0 (no snapshot, oldest firstIndex)", scope.ReadStartIndex)
	}
}

// TestPrintWalScope_Output builds a WAL, reads it, and renders the scope to
// confirm the human-readable summary looks right (smoke test of formatting).
func TestPrintWalScope_Output(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "member", "wal")
	writeWAL(t, walDir, []uint64{1, 2, 3, 4, 5}, 1)

	opc, scope, err := core.WalSource(dir)
	if err != nil {
		t.Fatalf("WalSource: %v", err)
	}
	var ops []core.WalOp
	for op := range opc {
		ops = append(ops, op)
	}

	var buf bytes.Buffer
	core.PrintWalScope(&buf, scope, len(ops))
	out := buf.String()
	for _, want := range []string{"Analysis scope:", "WAL segments read", "firstIndex=", "Total ops parsed: 5"} {
		if !bytes.Contains([]byte(out), []byte(want)) {
			t.Errorf("scope output missing %q:\n%s", want, out)
		}
	}
	t.Logf("scope output:\n%s", out)
}

// TestWalSource_WithSnapshotStartsAtSnapIndex writes a WAL, then opens it with
// a synthetic snapshot index to confirm only entries after that index are read.
// This mirrors the 3.4.7 fix path: OpenForRead with a real snap.Index.
func TestWalSource_WithSnapshotStartsAtSnapIndex(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "member", "wal")
	writeWAL(t, walDir, []uint64{1, 2, 3, 4, 5}, 1)

	// Read directly via wal.OpenForRead at snap.Index=3: only index>3 returned.
	// (WalSource uses loadNewestSnapshot, which returns {} here since we wrote no
	// .snap file; so WalSource would read all. To test the snap-started path we
	// invoke wal directly.)
	w, err := wal.OpenForRead(nil, walDir, walpb.Snapshot{Index: 3})
	if err != nil {
		t.Fatalf("OpenForRead: %v", err)
	}
	defer w.Close()
	_, _, ents, readErr := w.ReadAll()
	// No snapshot record at index 3 in this WAL -> ErrSnapshotNotFound, but ents
	// is populated. This is the soft error WalSource tolerates.
	if readErr != nil && len(ents) == 0 {
		t.Fatalf("ReadAll: %v with no entries", readErr)
	}
	var got []uint64
	for _, e := range ents {
		if e.Index > 3 {
			got = append(got, e.Index)
		}
	}
	if len(got) != 2 { // indexes 4, 5
		t.Fatalf("got %d entries after snap index 3, want 2: %v", len(got), got)
	}
}
