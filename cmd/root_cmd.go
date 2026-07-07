package cmd

import (
	"fmt"
	"os"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/spf13/cobra"
	cobracompletefig "github.com/withfig/autocomplete-tools/integrations/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "etcdctl+",
	Short: "etcd data analysis tool",
	Long: `etcd data analysis tool.

Two modes:

  Online (default): connect to etcd via --endpoints and scan live.
      etcdctl+ summary --group-depth=2 --sort=count --top=20
      etcdctl+ distribute --type=kv
      etcdctl+ find --match-key=starrocks

  Offline: analyze without contacting etcd. Two offline inputs:
    - summary / distribute / find: read a KeyMeta JSONL exported by
      'look --write-out=jsonl' (one analysis pass exports, many commands reuse):
        etcdctl+ look --snapshot=cluster.db --write-out=jsonl --output=keys.jsonl
        etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=count
        etcdctl+ distribute --input=keys.jsonl --type=kv
        etcdctl+ find --input=keys.jsonl --match-key=starrocks
    - look: parse a bbolt snapshot db directly (single pass, all fields
      including rev_count / tombstone_count):
        etcdctl+ look --snapshot=cluster.db

Offline is recommended for large clusters and batch inspection: snapshot once,
analyze many times, no sustained follower read pressure. Online is fine for
small clusters or ad-hoc queries.

Use <command> -h to see the flags each subcommand supports.
`,
}

func Start() {
	if err := rootCmd.Execute(); err != nil {
		if rootCmd.SilenceErrors {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(-1)
	}
}

func init() {
	cobra.EnablePrefixMatching = true

	rootCmd.PersistentFlags().StringSliceVar(&core.C.Endpoints, "endpoints", []string{"127.0.0.1:2379"}, "etcd connect Endpoints")
	rootCmd.PersistentFlags().StringVar(&core.C.TLS.CertFile, "cert", "", "identify secure client using this TLS certificate file")
	rootCmd.PersistentFlags().StringVar(&core.C.TLS.KeyFile, "key", "", "identify secure client using this TLS key file")
	rootCmd.PersistentFlags().StringVar(&core.C.TLS.TrustedCAFile, "cacert", "", "verify certificates of TLS-enabled secure servers using this CA bundle")
	rootCmd.PersistentFlags().IntVar(&core.C.CommandTimeout, "command-timeout", 5, "unit:s, the etcd operation will exit if the etcd server doesn't return value after the command timeout")

	rootCmd.AddCommand(NewDistributeCmd())
	rootCmd.AddCommand(NewLookCmd())
	rootCmd.AddCommand(NewLeaderCmd())
	// Disabled: high-risk commands that modify/delete etcd data
	// rootCmd.AddCommand(NewClearCmd())   // 🔴 deletes ALL etcd data, irreversible
	rootCmd.AddCommand(NewFindCmd())
	rootCmd.AddCommand(NewDecodeCmd())
	// rootCmd.AddCommand(NewRenameCmd())  // 🟠 non-atomic Get→Put→Delete, may cause inconsistency
	rootCmd.AddCommand(NewUnmarshalCmd())
	rootCmd.AddCommand(NewSummaryCmd())
	rootCmd.AddCommand(NewWalLookCmd())
	rootCmd.AddCommand(NewWalSummaryCmd())
	rootCmd.AddCommand(NewDumpCmd())
	rootCmd.AddCommand(cobracompletefig.CreateCompletionSpecCommand())
}
