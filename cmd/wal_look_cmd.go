package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/spf13/cobra"
)

var (
	walDataDir    string
	walWriteOut   string
	walOutput     string
	walStartIndex uint64
	walEndIndex   uint64
	walEntryType  string
)

// entryTypeFlagHelp is the shared --entry-type flag usage for wal-look and
// dump wal. Lists all 17 entry types and the op_type ↔ entry_type mapping.
const entryTypeFlagHelp = `Filter by entry type, comma-separated.

IRR types (op_type ↔ entry_type):
  Range           ↔ IRRRange          (read; not normally in WAL)
  Put             ↔ IRRPut
  DeleteRange     ↔ IRRDeleteRange
  Txn             ↔ IRRTxn
  Compaction      ↔ IRRCompaction
  LeaseGrant      ↔ IRRLeaseGrant
  LeaseRevoke     ↔ IRRLeaseRevoke
  LeaseCheckpoint ↔ IRRLeaseCheckpoint
  AuthEnable      ↔ IRRAuthEnable
  AuthDisable     ↔ IRRAuthDisable
  AuthUser        ↔ IRRAuthUser
  AuthRole        ↔ IRRAuthRole

Fallback: IRRUnknown, ConfigChange, Normal, Request, Unknown

e.g. IRRPut,IRRDeleteRange`

func NewWalLookCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wal-look",
		Short: "Export WAL operations for offline analysis",
		Long: `
Parse WAL log files and output decoded operations.

Each raft entry is decoded into one or more WalOp records containing
raft_index, raft_term, op_type, key, value_size_bytes, and entry_type.

By default the read starts at the newest snapshot's index (only post-snapshot
entries are returned, matching etcd's own recovery behavior). If no snapshot is
found, every available WAL segment is read from the oldest. An analysis-scope
summary (snapshot used, segments read/skipped, estimated time range) is printed
to stderr before the data.

Use --write-out=jsonl to produce WalOp JSONL for 'wal-summary --input'.

Examples:
  etcdctl+ wal-look --data-dir=/var/lib/etcd
  etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=jsonl --output=wal.jsonl
  etcdctl+ wal-look --data-dir=/var/lib/etcd --entry-type=IRRPut,IRRDeleteRange
  etcdctl+ wal-look --data-dir=/var/lib/etcd --start-index=100 --end-index=200
`,
		Run: walLookFunc,
	}

	cmd.Flags().StringVar(&walDataDir, "data-dir", "", "etcd data directory (required)")
	cmd.Flags().StringVar(&walWriteOut, "write-out", "stdout", "Output format: stdout, log, jsonl")
	cmd.Flags().StringVar(&walOutput, "output", "", "Output file path")
	cmd.Flags().Uint64Var(&walStartIndex, "start-index", 0, "Start raft index (inclusive)")
	cmd.Flags().Uint64Var(&walEndIndex, "end-index", math.MaxUint64, "End raft index (exclusive)")
	cmd.Flags().StringVar(&walEntryType, "entry-type", "", entryTypeFlagHelp)

	cmd.MarkFlagRequired("data-dir")

	cmd.RegisterFlagCompletionFunc("write-out", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"stdout", "log", "jsonl"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func walLookFunc(cmd *cobra.Command, args []string) {
	var opts []core.WalOption
	if walStartIndex > 0 {
		opts = append(opts, core.WithStartIndex(walStartIndex))
	}
	if walEndIndex < math.MaxUint64 {
		opts = append(opts, core.WithEndIndex(walEndIndex))
	}
	if walEntryType != "" {
		opts = append(opts, core.WithEntryTypeFilter(walEntryType))
	}

	opc, scope, err := core.WalSource(walDataDir, opts...)
	if err != nil {
		core.Exit(err)
	}

	// Collect into memory first so the scope summary (which includes the last
	// read index) can be printed before the data. The default post-snapshot read
	// is small (tens of thousands of entries); full-history reads (no snapshot)
	// are larger but still manageable for offline analysis.
	var ops []core.WalOp
	for op := range opc {
		ops = append(ops, op)
	}

	// Print the analysis-scope summary to stderr (does not pollute jsonl stdout
	// or the --output file). Printed before the data so the user sees context
	// first.
	core.PrintWalScope(os.Stderr, scope, len(ops))

	var writer *os.File
	switch walWriteOut {
	case "jsonl":
		writer = GetFileWriter(walOutput, "wal.jsonl")
		defer writer.Close()
	case "log":
		if walOutput != "" {
			writer = GetFileWriter(walOutput, "wal.log")
			defer writer.Close()
		} else {
			writer = os.Stdout
		}
	default:
		writer = os.Stdout
	}

	for _, op := range ops {
		switch walWriteOut {
		case "jsonl":
			b, _ := json.Marshal(op)
			fmt.Fprintln(writer, string(b))
		case "log":
			fmt.Fprintf(writer, "raft_index=%d raft_term=%d op_type=%s key=%s value_size_bytes=%d entry_type=%s\n",
				op.RaftIndex, op.RaftTerm, op.OpType, op.Key, op.ValueSizeBytes, op.EntryType)
		default:
			fmt.Fprintf(writer, "%-10d %-6d %-15s %-60s %8d  %s\n",
				op.RaftIndex, op.RaftTerm, op.OpType, op.Key, op.ValueSizeBytes, op.EntryType)
		}
	}
}
