package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	showValue    bool
	writeOut     string
	outputFile   string
	hang         bool
	hangInterval int64

	keysOnly bool
	lookPrefix string
	pageSize  int
	pageSleep time.Duration

	filterAttribute string
	filterMax       int
	filterMin       int
)

func NewLookCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "look",
		Short: "Look all etcd data",
		Long: `
Look all etcd data.

Considering that the value is generally encrypted and difficult to read and is relatively long, the value is not displayed by default.
If you want to display, you can set <show-value> to true. The print value has been decode by base64, you can decode command to decode the value.

By default, the command will output all the results to the console, which may be more practical in combination with some text viewing tools, such as 'more' or 'vim', like:
$ look | more
$ look | vim
Of course, you can also save the output as a file by setting <write-out> to 'file', and the generated file is named 'analysis.txt'.

If you want to continuously observe all keys, you can set <hang> to true, and you can use <hang-interval> to set the update interval.
This function only works when <write-out> is set to 'file'.

Sometimes, you may only want to observe data of a certain range size, you can set <filter> related parameters.
<filter> can set how to calculate the data size, including 'none', 'key', 'value' and 'kv';
<filter-max> and <filter-min> are used to specify the maximum and minimum values of the data, respectively.

Production tip: prefer '--keys-only' plus '--write-out=jsonl --output=<file>' for a low-risk snapshot,
then analyze the JSONL offline with 'summary --input=<file>'.

Combination rules:
  --keys-only --filter=none|key : allowed
  --keys-only --filter=value|kv : NOT allowed (no value to size)
  --keys-only --show-value      : NOT allowed (semantic conflict)
`,
		Run: lookFunc,
	}

	cmd.Flags().BoolVar(&showValue, "show-value", false, "Show the value or not (base64)")
	cmd.Flags().StringVar(&writeOut, "write-out", "stdout", "Output type: stdout, file, log, jsonl")
	cmd.Flags().StringVar(&outputFile, "output", "", "Output file path for file/log/jsonl (default analysis.txt or analysis.jsonl)")
	cmd.Flags().BoolVar(&hang, "hang", false, "Get updates periodically, only '--write-out=file' takes effect")
	cmd.Flags().Int64Var(&hangInterval, "hang-interval", 2, "Update interval, and the unit is 's'")
	cmd.Flags().StringVar(&filterAttribute, "filter", "none", "The filter attribute")
	cmd.Flags().IntVar(&filterMax, "filter-max", -1, "The filter max value")
	cmd.Flags().IntVar(&filterMin, "filter-min", -1, "The filter min value")

	cmd.Flags().BoolVar(&keysOnly, "keys-only", false, "Only fetch key metadata (no value); uses etcd WithKeysOnly()")
	cmd.Flags().StringVar(&lookPrefix, "prefix", "", "Only scan keys with the given prefix (server-side)")
	cmd.Flags().IntVar(&pageSize, "page-size", core.DefaultPageSize(), "Per-request page size")
	cmd.Flags().DurationVar(&pageSleep, "page-sleep", 0, "Sleep between pages, e.g. 50ms")

	cmd.RegisterFlagCompletionFunc("write-out", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"stdout", "file", "log", "jsonl"}, cobra.ShellCompDirectiveDefault
	})
	cmd.RegisterFlagCompletionFunc("filter", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"none", "key", "value", "kv"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func isLog() bool  { return writeOut == "log" }
func isJSONL() bool { return writeOut == "jsonl" }

// validateLookFlags enforces the combination rules from the improvement plan.
func validateLookFlags() {
	if err := core.CheckFilterCombination(keysOnly, filterAttribute); err != nil {
		core.Exit(err)
	}
	if keysOnly && showValue {
		core.Exit(fmt.Errorf("--keys-only cannot be combined with --show-value: semantic conflict"))
	}
}

// lookFilter returns the shared FilterConfig built from look's flags.
func lookFilter() core.FilterConfig {
	return core.FilterConfig{Attribute: filterAttribute, Min: filterMin, Max: filterMax}
}

func lookFunc(cmd *cobra.Command, args []string) {
	validateLookFlags()
	core.InitClient()

	scanOpts := []core.ScanOption{core.WithPageSize(pageSize)}
	if lookPrefix != "" {
		scanOpts = append(scanOpts, core.WithPrefix(lookPrefix))
	}
	if keysOnly {
		scanOpts = append(scanOpts, core.WithKeysOnly())
	}
	if pageSleep > 0 {
		scanOpts = append(scanOpts, core.WithPageSleep(pageSleep))
	}

	resp, datac := core.ScanData(scanOpts...)

	var writer io.Writer
	switch writeOut {
	case "file", "log":
		f := GetFileWriter(outputFile, "analysis.txt")
		defer f.Close()
		writer = f
	case "jsonl":
		f := GetFileWriter(outputFile, "analysis.jsonl")
		defer f.Close()
		writer = f
	case "stdout":
		fallthrough
	default:
		writer = os.Stdout
	}

	appendBuffer(resp, datac, writer)
	if hang && writeOut == "file" {
		ct := time.Tick(time.Second * time.Duration(hangInterval))
		i := 0
		for {
			select {
			case <-ct:
				resp, datac = core.ScanData(scanOpts...)
				appendBuffer(resp, datac, writer)
				fmt.Println(i, "flush...")
				i++
			}
		}
	}
}

func appendBuffer(resp *clientv3.GetResponse, datac <-chan []*mvccpb.KeyValue, writer io.Writer) {
	if f, ok := writer.(*os.File); ok {
		f.Truncate(0)
		f.Seek(0, 0)
	}
	var buffer bytes.Buffer
	if isJSONL() {
		drainJSONL(datac, writer)
		return
	}
	if !isLog() {
		buffer.WriteString("Current Stage\n")
		buffer.WriteString(fmt.Sprintf("  %s", resp.Header.String()))
		buffer.WriteString("\nKv List\n")
		buffer.WriteString("| Key | Value | KeySize | ValueSize | KvSize | CreateRevision | ModRevision | Version | Lease |\n")
	}

	for data := range datac {
		for _, kv := range data {
			if filterReject(kv) {
				continue
			}
			m := core.KVToMeta(kv, keysOnly, showValue)
			if isLog() {
				buffer.WriteString(core.FormatLogLine(m))
				buffer.WriteByte('\n')
				continue
			}
			buffer.WriteString(formatTableRow(m))
		}
	}

	buffer.WriteTo(writer)
}

// drainJSONL streams KVs to the writer as one JSON object per line without
// buffering the whole keyspace in memory.
func drainJSONL(datac <-chan []*mvccpb.KeyValue, writer io.Writer) {
	for data := range datac {
		for _, kv := range data {
			if filterReject(kv) {
				continue
			}
			m := core.KVToMeta(kv, keysOnly, showValue)
			fmt.Fprintln(writer, jsonlLine(m))
		}
	}
}

// jsonlLine marshals a KeyMeta to a compact JSON line.
func jsonlLine(m core.KeyMeta) string {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf(`{"key":%q,"error":%q}`, m.Key, err.Error())
	}
	return string(b)
}

func formatTableRow(m core.KeyMeta) string {
	v := "-"
	if m.Value != nil {
		v = *m.Value
	}
	if m.HasValue() {
		return fmt.Sprintf("| %s | %s | %d | %d | %d | %d | %d | %d | %d |\n",
			m.Key, v, m.KeySizeBytes, *m.ValueSizeBytes, *m.KvSizeBytes,
			m.CreateRevision, m.ModRevision, m.Version, m.Lease)
	}
	return fmt.Sprintf("| %s | %s | %d | - | - | %d | %d | %d | %d |\n",
		m.Key, v, m.KeySizeBytes,
		m.CreateRevision, m.ModRevision, m.Version, m.Lease)
}

// filterReject reports whether kv should be dropped by the --filter rules.
// In keys-only mode only 'key' filtering is meaningful; value/kv are rejected
// upstream by validateLookFlags.
func filterReject(kv *mvccpb.KeyValue) bool {
	return lookFilter().Reject(kv)
}

// GetFileWriter opens outputFile for writing, falling back to defaultName.
func GetFileWriter(outputFile, defaultName string) *os.File {
	name := outputFile
	if name == "" {
		name = defaultName
	}
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		core.Exit(err)
	}
	return f
}
