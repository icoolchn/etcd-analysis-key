package cmd

import (
	"encoding/json"
	"fmt"
	"io"
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

func NewWalLookCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wal-look",
		Short: "Export WAL operations for offline analysis",
		Long: `
Parse WAL log files and output decoded operations.

Each raft entry is decoded into one or more WalOp records containing
raft_index, raft_term, op_type, key, value_size_bytes, and entry_type.

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
	cmd.Flags().StringVar(&walEntryType, "entry-type", "", "Filter by entry type, comma-separated (e.g. IRRPut,IRRDeleteRange)")

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

	opc, err := core.WalSource(walDataDir, opts...)
	if err != nil {
		core.Exit(err)
	}

	var writer io.Writer
	switch walWriteOut {
	case "jsonl":
		f := GetFileWriter(walOutput, "wal.jsonl")
		defer f.Close()
		writer = f
	case "log":
		if walOutput != "" {
			f := GetFileWriter(walOutput, "wal.log")
			defer f.Close()
			writer = f
		} else {
			writer = os.Stdout
		}
	default:
		writer = os.Stdout
	}

	for op := range opc {
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
