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
	rootCmd.AddCommand(cobracompletefig.CreateCompletionSpecCommand())
}
