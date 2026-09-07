package core

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

// WalOp represents a single decoded WAL entry operation.
type WalOp struct {
	RaftIndex      uint64 `json:"raft_index"`
	RaftTerm       uint64 `json:"raft_term"`
	OpType         string `json:"op_type"`
	Key            string `json:"key,omitempty"`
	ValueSizeBytes int    `json:"value_size_bytes,omitempty"`
	IsTxn          bool   `json:"is_txn,omitempty"`
	EntryType      string `json:"entry_type,omitempty"`
}

// WalSegmentInfo describes one WAL segment file on disk.
type WalSegmentInfo struct {
	Name       string    // file name, e.g. 000000000000008f-0000000004989c51.wal
	Seq        uint64    // first field of the name
	FirstIndex uint64    // second field of the name
	Mtime      time.Time // file modification time = segment's last write
	IsTail     bool      // true for the highest-seq segment (still being written)
}

// SnapInfo describes one snapshot file on disk (filename + mtime only; the .snap
// content is NOT parsed here).
type SnapInfo struct {
	Name  string    // file name, e.g. 0000000000002250-00000000049b53a4.snap
	Term  uint64    // first field of the name
	Index uint64    // second field of the name
	Mtime time.Time // file modification time ≈ when the snapshot was taken
}

// WalScope describes what WalSource actually read: which snapshot was used as
// the start point, which segments were read vs skipped, and the resulting raft
// index range. Used to print an analysis-scope summary to the user.
type WalScope struct {
	// SnapUsed is the snapshot whose Index was passed to wal.OpenForRead as the
	// read start. May be empty (no snapshot found) -- then the oldest segment's
	// firstIndex is used and all segments are read.
	SnapUsed *SnapInfo
	// ReadStartIndex is the raft index passed to OpenForRead (snap.Index, or
	// oldest firstIndex when no snap). Entries with index > ReadStartIndex are
	// returned by ReadAll.
	ReadStartIndex uint64
	// ReadSegments are the WAL segments actually opened and read (from the one
	// containing ReadStartIndex to the tail).
	ReadSegments []WalSegmentInfo
	// SkippedSegments are the WAL segments before ReadStartIndex (not read).
	SkippedSegments []WalSegmentInfo
}

// WalOption configures a WalSource call.
type WalOption func(*walConfig)

type walConfig struct {
	startIndex uint64
	endIndex   uint64
	entryTypes map[string]bool
}

// WithStartIndex sets the inclusive start raft index.
func WithStartIndex(idx uint64) WalOption {
	return func(c *walConfig) { c.startIndex = idx }
}

// WithEndIndex sets the exclusive end raft index.
func WithEndIndex(idx uint64) WalOption {
	return func(c *walConfig) { c.endIndex = idx }
}

// WithEntryTypeFilter restricts output to the given entry types (comma-separated).
func WithEntryTypeFilter(types string) WalOption {
	return func(c *walConfig) {
		c.entryTypes = make(map[string]bool)
		for _, t := range strings.Split(types, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				c.entryTypes[t] = true
			}
		}
	}
}

// loadNewestSnapshot reads the newest snapshot under <walPath>/../snap and
// returns its raft Index/Term. An empty Snapshot is returned when there is no
// snapshot (e.g. a fresh cluster that has never snapshotted, or only member/wal
// was copied without member/snap), so the caller falls back to reading from the
// first WAL segment.
//
// The real snapshot index is required because wal.OpenForRead calls searchIndex
// to land on the segment containing snap.Index. With a zero snapshot index it
// only matches a first segment named ...-0000000000000000.wal; once that segment
// is purged after compaction (the common case for a long-running cluster),
// searchIndex returns ErrFileNotFound ("wal: file not found").
func loadNewestSnapshot(walPath string) walpb.Snapshot {
	snapDir := filepath.Join(walPath, "..", "snap")
	if !isDir(snapDir) {
		return walpb.Snapshot{}
	}
	s, err := snap.New(nil, snapDir).Load()
	if err != nil || s == nil {
		return walpb.Snapshot{}
	}
	return walpb.Snapshot{Index: s.Metadata.Index, Term: s.Metadata.Term}
}

// resolveWalDir resolves --data-dir to the actual WAL directory.
// Accepts either an etcd data directory (containing member/wal) or a WAL
// directory (containing *.wal segment files) directly.
func resolveWalDir(dataDir string) (string, error) {
	// 1. etcd data dir: <dataDir>/member/wal
	if candidate := filepath.Join(dataDir, "member", "wal"); isDir(candidate) {
		return candidate, nil
	}
	// 2. already a WAL dir: contains *.wal files
	if entries, err := os.ReadDir(dataDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
				return dataDir, nil
			}
		}
	}
	return "", fmt.Errorf("--data-dir %q: not an etcd data dir (no member/wal) nor a WAL dir (no *.wal files)", dataDir)
}

func isDir(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}

// listWalSegments scans <walPath>/*.wal and returns segment infos sorted by Seq
// ascending. The highest-Seq segment is marked IsTail (still being written).
func listWalSegments(walPath string) []WalSegmentInfo {
	entries, err := os.ReadDir(walPath)
	if err != nil {
		return nil
	}
	var segs []WalSegmentInfo
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		seq, idx, ok := parseWalSnapName(e.Name(), ".wal")
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		segs = append(segs, WalSegmentInfo{
			Name:       e.Name(),
			Seq:        seq,
			FirstIndex: idx,
			Mtime:      info.ModTime(),
		})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].Seq < segs[j].Seq })
	if len(segs) > 0 {
		segs[len(segs)-1].IsTail = true
	}
	return segs
}

// listSnaps scans <walPath>/../snap/*.snap and returns snap infos sorted by
// Index ascending. Only filenames and mtimes are read; .snap content is NOT
// parsed.
func listSnaps(walPath string) []SnapInfo {
	snapDir := filepath.Join(walPath, "..", "snap")
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return nil
	}
	var snaps []SnapInfo
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".snap") {
			continue
		}
		term, idx, ok := parseWalSnapName(e.Name(), ".snap")
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		snaps = append(snaps, SnapInfo{
			Name:  e.Name(),
			Term:  term,
			Index: idx,
			Mtime: info.ModTime(),
		})
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Index < snaps[j].Index })
	return snaps
}

// parseWalSnapName parses a "{f0}-{f1}<suffix>" filename (wal or snap) and
// returns both hex fields as uint64. For wal: f0=seq, f1=firstIndex. For snap:
// f0=term, f1=index.
func parseWalSnapName(name, suffix string) (f0, f1 uint64, ok bool) {
	base := strings.TrimSuffix(name, suffix)
	parts := strings.SplitN(base, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	a, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, false
	}
	b, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return 0, 0, false
	}
	return a, b, true
}

// WalSource opens WAL files in the given data directory and streams decoded
// WalOp entries. Uses wal.OpenForRead + ReadAll. The read starts at the newest
// snapshot's index (only post-snapshot entries are returned, matching etcd's own
// recovery behavior); if no snapshot is found, it falls back to the oldest
// segment's firstIndex and reads every segment. The returned WalScope describes
// what was actually read, for printing an analysis-scope summary.
//
// ReadAll is allowed to return wal.ErrSnapshotNotFound (a soft error: entries
// are still populated) when the start index has no matching snapshot record in
// the WAL; it is tolerated. Other read errors abort the stream.
func WalSource(dataDir string, opts ...WalOption) (<-chan WalOp, *WalScope, error) {
	cfg := &walConfig{endIndex: ^uint64(0)}
	for _, o := range opts {
		o(cfg)
	}

	walPath, err := resolveWalDir(dataDir)
	if err != nil {
		return nil, nil, err
	}

	scope := &WalScope{}
	segs := listWalSegments(walPath)
	snaps := listSnaps(walPath)

	// Read start: newest snapshot's index if available, else oldest segment's
	// firstIndex (read everything). This mirrors etcd recovery: the snapshot
	// already archives all entries up to its index, so only post-snap entries
	// are read from the WAL.
	walsnap := loadNewestSnapshot(walPath)
	if walsnap.Index == 0 && len(segs) > 0 {
		// No snapshot: start from the oldest segment's firstIndex.
		walsnap = walpb.Snapshot{Index: segs[0].FirstIndex}
	}
	scope.ReadStartIndex = walsnap.Index
	if len(snaps) > 0 {
		newest := snaps[len(snaps)-1]
		// SnapUsed is the snapshot whose index matches the read start (the newest
		// one, since loadNewestSnapshot returns it).
		for i := range snaps {
			if snaps[i].Index == walsnap.Index {
				s := snaps[i]
				scope.SnapUsed = &s
				break
			}
		}
		_ = newest
	}

	// Split segments into read (containing ReadStartIndex .. tail) vs skipped.
	readStarted := false
	for i := range segs {
		if !readStarted {
			// The first segment whose firstIndex <= ReadStartIndex + (it's the
			// last such, since segs are sorted by seq and firstIndex increases)
			// is where OpenForRead's searchIndex lands.
			if i == len(segs)-1 || segs[i+1].FirstIndex > scope.ReadStartIndex {
				readStarted = true
				scope.ReadSegments = append(scope.ReadSegments, segs[i])
			} else {
				scope.SkippedSegments = append(scope.SkippedSegments, segs[i])
			}
		} else {
			scope.ReadSegments = append(scope.ReadSegments, segs[i])
		}
	}

	w, err := wal.OpenForRead(nil, walPath, walsnap)
	if err != nil {
		if errors.Is(err, wal.ErrFileNotFound) {
			return nil, nil, fmt.Errorf("open WAL: %w (cluster may have snapshotted and purged old WAL; "+
				"ensure member/snap is copied alongside member/wal)", err)
		}
		return nil, nil, fmt.Errorf("open WAL: %w", err)
	}

	c := make(chan WalOp, 100)
	go func() {
		defer close(c)
		defer w.Close()

		_, _, ents, readErr := w.ReadAll()
		// ErrSnapshotNotFound is a soft error: the start index has no matching
		// snapshot record in the WAL, but ents is still populated.
		if readErr != nil && !errors.Is(readErr, wal.ErrSnapshotNotFound) {
			return
		}

		for _, e := range ents {
			if e.Index < cfg.startIndex || e.Index >= cfg.endIndex {
				continue
			}
			ops := decodeEntry(e)
			for _, op := range ops {
				if len(cfg.entryTypes) > 0 && !cfg.entryTypes[op.EntryType] {
					continue
				}
				c <- op
			}
		}
	}()

	return c, scope, nil
}

// decodeEntry decodes a single raft entry into one or more WalOps.
func decodeEntry(e raftpb.Entry) []WalOp {
	if e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 {
		return []WalOp{{
			RaftIndex: e.Index,
			RaftTerm:  e.Term,
			OpType:    "ConfigChange",
			EntryType: "ConfigChange",
		}}
	}

	if len(e.Data) == 0 {
		return []WalOp{{
			RaftIndex: e.Index,
			RaftTerm:  e.Term,
			OpType:    "Normal",
			EntryType: "Normal",
		}}
	}

	var raftReq etcdserverpb.InternalRaftRequest
	if err := proto.Unmarshal(e.Data, &raftReq); err != nil {
		var req etcdserverpb.Request
		if err2 := proto.Unmarshal(e.Data, &req); err2 != nil {
			return []WalOp{{
				RaftIndex: e.Index,
				RaftTerm:  e.Term,
				OpType:    "Unknown",
				EntryType: "Unknown",
			}}
		}
		return []WalOp{{
			RaftIndex: e.Index,
			RaftTerm:  e.Term,
			OpType:    "Request",
			Key:       req.Path,
			EntryType: "Request",
		}}
	}

	return decodeIRR(e.Index, e.Term, &raftReq)
}

func decodeIRR(index, term uint64, rr *etcdserverpb.InternalRaftRequest) []WalOp {
	var ops []WalOp
	base := WalOp{RaftIndex: index, RaftTerm: term}

	if rr.Range != nil {
		op := base
		op.OpType = "Range"
		op.Key = string(rr.Range.Key)
		op.EntryType = "IRRRange"
		ops = append(ops, op)
	}
	if rr.Put != nil {
		op := base
		op.OpType = "Put"
		op.Key = string(rr.Put.Key)
		op.ValueSizeBytes = len(rr.Put.Value)
		op.EntryType = "IRRPut"
		ops = append(ops, op)
	}
	if rr.DeleteRange != nil {
		op := base
		op.OpType = "DeleteRange"
		op.Key = string(rr.DeleteRange.Key)
		op.EntryType = "IRRDeleteRange"
		ops = append(ops, op)
	}
	if rr.Txn != nil {
		op := base
		op.OpType = "Txn"
		op.IsTxn = true
		op.EntryType = "IRRTxn"
		txnOps := decodeTxnOps(index, term, rr.Txn)
		if len(txnOps) > 0 {
			op.Key = txnOps[0].Key
		}
		ops = append(ops, op)
		ops = append(ops, txnOps...)
	}
	if rr.Compaction != nil {
		op := base
		op.OpType = "Compaction"
		op.EntryType = "IRRCompaction"
		ops = append(ops, op)
	}
	if rr.LeaseGrant != nil {
		op := base
		op.OpType = "LeaseGrant"
		op.EntryType = "IRRLeaseGrant"
		ops = append(ops, op)
	}
	if rr.LeaseRevoke != nil {
		op := base
		op.OpType = "LeaseRevoke"
		op.EntryType = "IRRLeaseRevoke"
		ops = append(ops, op)
	}
	if rr.LeaseCheckpoint != nil {
		op := base
		op.OpType = "LeaseCheckpoint"
		op.EntryType = "IRRLeaseCheckpoint"
		ops = append(ops, op)
	}
	if rr.AuthEnable != nil {
		op := base
		op.OpType = "AuthEnable"
		op.EntryType = "IRRAuthEnable"
		ops = append(ops, op)
	}
	if rr.AuthDisable != nil {
		op := base
		op.OpType = "AuthDisable"
		op.EntryType = "IRRAuthDisable"
		ops = append(ops, op)
	}
	if rr.AuthUserAdd != nil || rr.AuthUserDelete != nil || rr.AuthUserChangePassword != nil || rr.AuthUserGrantRole != nil || rr.AuthUserRevokeRole != nil {
		op := base
		op.OpType = "AuthUser"
		op.EntryType = "IRRAuthUser"
		ops = append(ops, op)
	}
	if rr.AuthRoleAdd != nil || rr.AuthRoleDelete != nil || rr.AuthRoleGrantPermission != nil || rr.AuthRoleRevokePermission != nil {
		op := base
		op.OpType = "AuthRole"
		op.EntryType = "IRRAuthRole"
		ops = append(ops, op)
	}

	if len(ops) == 0 {
		op := base
		op.OpType = "InternalRaftRequest"
		op.EntryType = "IRRUnknown"
		ops = append(ops, op)
	}

	return ops
}

func decodeTxnOps(index, term uint64, txn *etcdserverpb.TxnRequest) []WalOp {
	var ops []WalOp
	for _, reqOp := range append(txn.Success, txn.Failure...) {
		switch op := reqOp.Request.(type) {
		case *etcdserverpb.RequestOp_RequestPut:
			if op.RequestPut != nil {
				ops = append(ops, WalOp{
					RaftIndex:      index,
					RaftTerm:       term,
					OpType:         "Put",
					Key:            string(op.RequestPut.Key),
					ValueSizeBytes: len(op.RequestPut.Value),
					EntryType:      "IRRTxn",
					IsTxn:          true,
				})
			}
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			if op.RequestDeleteRange != nil {
				ops = append(ops, WalOp{
					RaftIndex: index,
					RaftTerm:  term,
					OpType:    "DeleteRange",
					Key:       string(op.RequestDeleteRange.Key),
					EntryType: "IRRTxn",
					IsTxn:     true,
				})
			}
		}
	}
	return ops
}

// CollectWalOps reads all WalOps from WAL files into memory.
func CollectWalOps(dataDir string, opts ...WalOption) ([]WalOp, error) {
	opc, _, err := WalSource(dataDir, opts...)
	if err != nil {
		return nil, err
	}
	var ops []WalOp
	for op := range opc {
		ops = append(ops, op)
	}
	return ops, nil
}

// CollectWalOpsWithScope is like CollectWalOps but also returns the WalScope
// describing what was read (used to print an analysis-scope summary).
func CollectWalOpsWithScope(dataDir string, opts ...WalOption) ([]WalOp, *WalScope, error) {
	opc, scope, err := WalSource(dataDir, opts...)
	if err != nil {
		return nil, nil, err
	}
	var ops []WalOp
	for op := range opc {
		ops = append(ops, op)
	}
	return ops, scope, nil
}

// PrintWalScope writes a human-readable summary of what WalSource read to w
// (usually stderr): the snapshot used as the read start, the WAL segments read
// vs skipped, and the estimated write-time range. totalOps is the number of ops
// actually decoded, used for the final summary line.
//
// The time range is an estimate: raft entries carry no timestamp, so the lower
// bound is the snapshot's mtime (≈ when its index was applied) and the upper
// bound is the tail segment's mtime (last known write). Same-segment entries
// cannot be distinguished in time.
func PrintWalScope(w io.Writer, scope *WalScope, totalOps int) {
	if scope == nil {
		return
	}
	fmt.Fprintln(w, "Analysis scope:")
	fmt.Fprintln(w)

	if scope.SnapUsed != nil {
		s := scope.SnapUsed
		fmt.Fprintf(w, "  Start (newest snapshot): %s  index=%d  term=%d  mtime=%s\n",
			s.Name, s.Index, s.Term, s.Mtime.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(w, "  (entries with raft_index > %d are read; earlier ones are archived in the snapshot)\n",
			s.Index)
	} else {
		fmt.Fprintf(w, "  Start (no snapshot, oldest segment firstIndex): raft_index > %d\n", scope.ReadStartIndex)
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "  WAL segments read (%d):\n", len(scope.ReadSegments))
	for _, seg := range scope.ReadSegments {
		tail := ""
		if seg.IsTail {
			tail = "  (tail, still being written)"
		}
		fmt.Fprintf(w, "    %s  firstIndex=%d  lastWrite=%s%s\n",
			seg.Name, seg.FirstIndex, seg.Mtime.Format("2006-01-02 15:04:05"), tail)
	}
	fmt.Fprintln(w)

	if len(scope.SkippedSegments) > 0 {
		fmt.Fprintf(w, "  WAL segments skipped (%d, before the read start):\n", len(scope.SkippedSegments))
		for _, seg := range scope.SkippedSegments {
			fmt.Fprintf(w, "    %s  firstIndex=%d  lastWrite=%s\n",
				seg.Name, seg.FirstIndex, seg.Mtime.Format("2006-01-02 15:04:05"))
		}
		fmt.Fprintln(w)
	}

	var lowerTime, upperTime string
	if scope.SnapUsed != nil {
		lowerTime = scope.SnapUsed.Mtime.Format("2006-01-02 15:04")
	} else if len(scope.SkippedSegments) > 0 {
		lowerTime = scope.SkippedSegments[0].Mtime.Format("2006-01-02 15:04")
	} else if len(scope.ReadSegments) > 0 {
		lowerTime = scope.ReadSegments[0].Mtime.Format("2006-01-02 15:04")
	}
	if len(scope.ReadSegments) > 0 {
		tail := scope.ReadSegments[len(scope.ReadSegments)-1]
		upperTime = tail.Mtime.Format("2006-01-02 15:04")
	}
	if lowerTime != "" && upperTime != "" {
		fmt.Fprintf(w, "  Estimated write-time range: %s ~ %s\n", lowerTime, upperTime)
		fmt.Fprintln(w, "  (raft entries carry no timestamp; inferred from snapshot/wal file mtimes)")
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "  Total ops parsed: %d\n", totalOps)
}

// ReadWalJSONL reads a WalOp JSONL file.
func ReadWalJSONL(path string) ([]WalOp, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ops []WalOp
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var op WalOp
		if err := json.Unmarshal([]byte(line), &op); err != nil {
			return nil, fmt.Errorf("invalid WalOp jsonl at line %d: %w", lineNo, err)
		}
		ops = append(ops, op)
	}
	return ops, sc.Err()
}

// WriteWalJSONL writes WalOps as one JSON object per line to w.
func WriteWalJSONL(ops []WalOp, w io.Writer) error {
	bw := bufio.NewWriter(w)
	for _, op := range ops {
		b, err := json.Marshal(op)
		if err != nil {
			return err
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// WalKeyStats holds per-key aggregation from WAL operations.
type WalKeyStats struct {
	Key                 string `json:"key"`
	PutCount            int    `json:"put_count"`
	DeleteCount         int    `json:"delete_count"`
	TotalValueSizeBytes int64  `json:"total_value_size_bytes"`
}

// AggregateWalOps aggregates WalOps by key, returning per-key stats.
func AggregateWalOps(ops []WalOp) []WalKeyStats {
	m := make(map[string]*WalKeyStats)
	order := make([]string, 0)
	for _, op := range ops {
		if op.Key == "" {
			continue
		}
		s, ok := m[op.Key]
		if !ok {
			s = &WalKeyStats{Key: op.Key}
			m[op.Key] = s
			order = append(order, op.Key)
		}
		switch op.OpType {
		case "Put":
			s.PutCount++
			s.TotalValueSizeBytes += int64(op.ValueSizeBytes)
		case "DeleteRange":
			s.DeleteCount++
		}
	}
	result := make([]WalKeyStats, 0, len(order))
	for _, k := range order {
		result = append(result, *m[k])
	}
	return result
}

// SortWalKeyStats sorts by the given key (put-count, delete-count, total-ops)
// in descending order.
func SortWalKeyStats(stats []WalKeyStats, sortBy string) {
	less := func(i, j int) bool { return stats[i].PutCount > stats[j].PutCount }
	switch sortBy {
	case "delete-count":
		less = func(i, j int) bool { return stats[i].DeleteCount > stats[j].DeleteCount }
	case "total-ops":
		less = func(i, j int) bool {
			return (stats[i].PutCount + stats[i].DeleteCount) > (stats[j].PutCount + stats[j].DeleteCount)
		}
	}
	sort.Slice(stats, less)
}

// EntryTypeCount is one row of the entry-type distribution.
type EntryTypeCount struct {
	EntryType string `json:"entry_type"`
	Count     int    `json:"count"`
}

// entryTypeOrder defines the display order of entry types in the distribution
// table: write-pressure types first (Put/Delete/Txn/Compaction), then lease and
// auth, then the read type (Range, not normally in WAL), then fallback types.
var entryTypeOrder = []string{
	"IRRPut", "IRRDeleteRange", "IRRTxn", "IRRCompaction",
	"IRRLeaseGrant", "IRRLeaseRevoke", "IRRLeaseCheckpoint",
	"IRRAuthEnable", "IRRAuthDisable", "IRRAuthUser", "IRRAuthRole",
	"IRRRange",
	"IRRUnknown", "ConfigChange", "Normal", "Request", "Unknown",
}

// EntryTypeDist counts ops per entry_type, returning rows in entryTypeOrder
// (any unseen type appended at the end in stable order). Empty entry_type is
// skipped.
func EntryTypeDist(ops []WalOp) []EntryTypeCount {
	m := make(map[string]int)
	for _, op := range ops {
		if op.EntryType == "" {
			continue
		}
		m[op.EntryType]++
	}
	result := make([]EntryTypeCount, 0, len(m))
	seen := make(map[string]bool, len(m))
	for _, t := range entryTypeOrder {
		if c, ok := m[t]; ok {
			result = append(result, EntryTypeCount{EntryType: t, Count: c})
			seen[t] = true
		}
	}
	// Append any unexpected types not in entryTypeOrder, sorted for stability.
	var extra []string
	for t := range m {
		if !seen[t] {
			extra = append(extra, t)
		}
	}
	sort.Strings(extra)
	for _, t := range extra {
		result = append(result, EntryTypeCount{EntryType: t, Count: m[t]})
	}
	return result
}
