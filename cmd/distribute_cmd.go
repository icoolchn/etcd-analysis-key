package cmd

import (
	"fmt"
	"time"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	distributeType     string
	bucketCount        int
	distributeWriteOut string
	distributePrefix   string
	distributePageSize int
	distributePageSleep time.Duration
)

func NewDistributeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "distribute",
		Short: "Show the data distribution of etcd",
		Long: `
Show the data distribution of etcd.

According to setting <type>, this command will show the data distribution by the size of the <type>.
The 'kv' means the 'key' and 'value'.

According to setting <bucket>, this command will show the different size histogram.
Each size interval is '(maxSize - minSize) / bucket'.

According to the output below, it means:
when the data size is '0.0 B', the count of this kind of data is 12.
when the data size is greater than '0.0B' and less than or equal to '573.0 B', the count is 275.
'573.0 B' < size <= '1.1 KiB', count 80.

Example:
$ distribute --type=value --bucket=8
Summary:
  Count:        399.
  Total:        267.9 KiB.
  Smallest:     0.0 B.
  Largest:      4.5 KiB.
  Average:      687.0 B.

Size histogram:
  0.0 B [12]    |∎
  573.0 B [275] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
  1.1 KiB [80]   |∎∎∎∎∎∎∎∎∎∎
  1.7 KiB [0]    |
  2.2 KiB [0]    |
  2.8 KiB [0]    |
  3.4 KiB [0]    |
  3.9 KiB [0]    |
  4.5 KiB [32]   |∎∎∎∎

Size distribution:
  10% in 3.0 B.
  25% in 4.0 B.
  50% in 424.0 B.
  75% in 854.0 B.
  90% in 1.0 KiB.
  95% in 4.5 KiB.
  99% in 4.5 KiB.
`,
		Run: distributeFunc,
	}

	cmd.Flags().StringVar(&distributeType, "type", "key", "Distribution basis; key, value or kv")
	cmd.Flags().IntVar(&bucketCount, "bucket", 5, "Bucket Count")
	cmd.Flags().StringVar(&distributeWriteOut, "write-out", "text", "Output format: text or json")
	cmd.Flags().StringVar(&distributePrefix, "prefix", "", "Only scan keys with the given prefix (server-side)")
	cmd.Flags().IntVar(&distributePageSize, "page-size", core.DefaultPageSize(), "Per-request page size")
	cmd.Flags().DurationVar(&distributePageSleep, "page-sleep", 0, "Sleep between pages, e.g. 50ms")

	cmd.RegisterFlagCompletionFunc("write-out", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"text", "json"}, cobra.ShellCompDirectiveDefault
	})

	cmd.RegisterFlagCompletionFunc("type", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"key", "value", "kv"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func distributeFunc(cmd *cobra.Command, args []string) {
	core.InitClient()
	scanOpts := []core.ScanOption{core.WithPageSize(distributePageSize)}
	if distributePrefix != "" {
		scanOpts = append(scanOpts, core.WithPrefix(distributePrefix))
	}
	if distributePageSleep > 0 {
		scanOpts = append(scanOpts, core.WithPageSleep(distributePageSleep))
	}
	_, datac := core.ScanData(scanOpts...)

	sizeOf := func(kv *mvccpb.KeyValue) int {
		switch distributeType {
		case "value":
			return len(kv.Value)
		case "kv":
			return len(kv.Key) + len(kv.Value)
		case "key":
			fallthrough
		default:
			return len(kv.Key)
		}
	}

	isJSON := distributeWriteOut == "json"

	// Build report with appropriate options
	var r core.Report
	if isJSON {
		r = core.NewReport(bucketCount, sizeOf, core.WithJSONMode())
	} else {
		r = core.NewReport(bucketCount, sizeOf)
	}

	// Common data pipeline
	c1 := r.Results()
	go func() {
		defer close(c1)
		if !isJSON && len(datac) > 0 {
			r.DynamicOutput()
		}
		for data := range datac {
			c1 <- data
		}
	}()
	<-r.Run()

	// Output
	if isJSON {
		fmt.Println(r.JSON())
	}
}
