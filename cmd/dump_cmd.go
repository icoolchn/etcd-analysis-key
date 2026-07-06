package cmd

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	dumpSnapshot string
	dumpDataDir  string
)

func NewDumpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Raw data export from snapshot db or WAL",
		Long: `
Raw data export commands for snapshot db and WAL files.
Outputs plain text for human inspection. For structured analysis, use
'look --snapshot' or 'wal-look' with --write-out=jsonl instead.
`,
	}

	cmd.AddCommand(newDumpListBucketCmd())
	cmd.AddCommand(newDumpIterateBucketCmd())
	cmd.AddCommand(newDumpScanKeysCmd())
	cmd.AddCommand(newDumpWalCmd())

	return cmd
}

// --- list-bucket ---

func newDumpListBucketCmd() *cobra.Command {
	var snapshot string
	cmd := &cobra.Command{
		Use:   "list-bucket",
		Short: "List all bucket names in a snapshot db",
		Run: func(cmd *cobra.Command, args []string) {
			names, err := core.ListBuckets(snapshot)
			if err != nil {
				core.Exit(err)
			}
			for _, n := range names {
				fmt.Println(n)
			}
		},
	}
	cmd.Flags().StringVar(&snapshot, "snapshot", "", "Snapshot db file path (required)")
	cmd.MarkFlagRequired("snapshot")
	return cmd
}

// --- iterate-bucket ---

func newDumpIterateBucketCmd() *cobra.Command {
	var (
		snapshot string
		limit    int
		decode   bool
	)
	cmd := &cobra.Command{
		Use:   "iterate-bucket [bucket-name]",
		Short: "Iterate entries in a bucket with size info",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			bucketName := args[0]
			entries, err := core.IterateBucket(snapshot, bucketName, core.IterateBucketConfig{
				Limit:  limit,
				Decode: decode,
			})
			if err != nil {
				core.Exit(err)
			}
			for i, e := range entries {
				fmt.Printf("[%d] key_size=%d value_size=%d\n", i, e.KeySizeBytes, e.ValueSizeBytes)
				if decode && bucketName == "key" {
					rev, err := core.BytesToBucketKey(e.Key)
					if err == nil {
						fmt.Printf("     rev=%d.%d tombstone=%v\n", rev.Main, rev.Sub, rev.Tombstone)
					}
					kv := &mvccpb.KeyValue{}
					if err := proto.Unmarshal(e.Value, kv); err == nil {
						fmt.Printf("     key=%s value_size=%d create_rev=%d mod_rev=%d version=%d lease=%d\n",
							string(kv.Key), len(kv.Value), kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease)
					}
				} else if decode {
					fmt.Printf("     key=%s\n", string(e.Key))
					fmt.Printf("     value=%s\n", truncate(string(e.Value), 200))
				} else {
					fmt.Printf("     key=%x\n", e.Key)
				}
			}
			fmt.Printf("\nTotal: %d entries\n", len(entries))
		},
	}
	cmd.Flags().StringVar(&snapshot, "snapshot", "", "Snapshot db file path (required)")
	cmd.Flags().IntVar(&limit, "limit", 0, "Max entries to show (0 = all)")
	cmd.Flags().BoolVar(&decode, "decode", false, "Decode key bucket entries as mvccpb.KeyValue")
	cmd.MarkFlagRequired("snapshot")
	return cmd
}

// --- scan-keys ---

func newDumpScanKeysCmd() *cobra.Command {
	var (
		snapshot      string
		startRevision int64
		endRevision   int64
		limit         int
	)
	cmd := &cobra.Command{
		Use:   "scan-keys",
		Short: "Scan keys by revision range in a snapshot db",
		Run: func(cmd *cobra.Command, args []string) {
			keys, err := core.ScanKeys(snapshot, core.ScanKeysConfig{
				StartRevision: startRevision,
				EndRevision:   endRevision,
				Limit:         limit,
			})
			if err != nil {
				core.Exit(err)
			}
			for _, k := range keys {
				tomb := ""
				if k.Rev.Tombstone {
					tomb = " [tombstone]"
				}
				fmt.Printf("rev=%d.%d%s key=%s value_size=%d create_rev=%d mod_rev=%d version=%d\n",
					k.Rev.Main, k.Rev.Sub, tomb,
					string(k.KV.Key), len(k.KV.Value),
					k.KV.CreateRevision, k.KV.ModRevision, k.KV.Version)
			}
			fmt.Printf("\nTotal: %d entries\n", len(keys))
		},
	}
	cmd.Flags().StringVar(&snapshot, "snapshot", "", "Snapshot db file path (required)")
	cmd.Flags().Int64Var(&startRevision, "start-revision", 0, "Start revision (inclusive, 0 = unbounded)")
	cmd.Flags().Int64Var(&endRevision, "end-revision", 0, "End revision (inclusive, 0 = unbounded)")
	cmd.Flags().IntVar(&limit, "limit", 0, "Max entries to show (0 = all)")
	cmd.MarkFlagRequired("snapshot")
	return cmd
}

// --- wal ---

func newDumpWalCmd() *cobra.Command {
	var (
		dataDir    string
		entryType  string
		startIndex uint64
		endIndex   uint64
	)
	cmd := &cobra.Command{
		Use:   "wal",
		Short: "Dump WAL entries as plain text",
		Run: func(cmd *cobra.Command, args []string) {
			var opts []core.WalOption
			if startIndex > 0 {
				opts = append(opts, core.WithStartIndex(startIndex))
			}
			if endIndex < math.MaxUint64 {
				opts = append(opts, core.WithEndIndex(endIndex))
			}
			if entryType != "" {
				opts = append(opts, core.WithEntryTypeFilter(entryType))
			}

			opc, err := core.WalSource(dataDir, opts...)
			if err != nil {
				core.Exit(err)
			}

			count := 0
			for op := range opc {
				data := ""
				if op.ValueSizeBytes > 0 {
					data = fmt.Sprintf(" value_size=%d", op.ValueSizeBytes)
				}
				txn := ""
				if op.IsTxn {
					txn = " [txn]"
				}
				fmt.Printf("index=%-10d term=%-6d type=%-15s key=%s%s%s\n",
					op.RaftIndex, op.RaftTerm, op.EntryType, op.Key, data, txn)
				count++
			}
			fmt.Printf("\nTotal: %d entries\n", count)
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "etcd data directory (required)")
	cmd.Flags().StringVar(&entryType, "entry-type", "", entryTypeFlagHelp)
	cmd.Flags().Uint64Var(&startIndex, "start-index", 0, "Start raft index (inclusive)")
	cmd.Flags().Uint64Var(&endIndex, "end-index", math.MaxUint64, "End raft index (exclusive)")
	cmd.MarkFlagRequired("data-dir")
	return cmd
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// Suppress unused import warning — json is used by other cmd files in this package
// but iterate-bucket's decode path uses proto.Unmarshal.
var _ = json.Marshal
