package core

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
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

// WalOption configures a WalSource call.
type WalOption func(*walConfig)

type walConfig struct {
	startIndex  uint64
	endIndex    uint64
	entryTypes  map[string]bool
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

func loadNewestSnapshot(dataDir string) walpb.Snapshot {
	snapDir := filepath.Join(dataDir, "member", "snap")
	names, err := filepath.Glob(filepath.Join(snapDir, "*.snap"))
	if err != nil || len(names) == 0 {
		return walpb.Snapshot{}
	}
	sort.Strings(names)
	return walpb.Snapshot{}
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

// WalSource opens WAL files in the given data directory and streams decoded
// WalOp entries. Uses wal.OpenForRead + ReadAll.
func WalSource(dataDir string, opts ...WalOption) (<-chan WalOp, error) {
	cfg := &walConfig{endIndex: ^uint64(0)}
	for _, o := range opts {
		o(cfg)
	}

	walsnap := loadNewestSnapshot(dataDir)
	walPath, err := resolveWalDir(dataDir)
	if err != nil {
		return nil, err
	}
	w, err := wal.OpenForRead(nil, walPath, walsnap)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	c := make(chan WalOp, 100)
	go func() {
		defer close(c)
		defer w.Close()

		_, _, ents, readErr := w.ReadAll()
		if readErr != nil {
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

	return c, nil
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
	opc, err := WalSource(dataDir, opts...)
	if err != nil {
		return nil, err
	}
	var ops []WalOp
	for op := range opc {
		ops = append(ops, op)
	}
	return ops, nil
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
