package cmd

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	findKey      = ""
	findPrefix   = ""
	findInput    = ""
	containValue = false
	findLimit    = 10
)

func NewFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "find the key from all etcd data",
		Run:   findFunc,
	}

	cmd.Flags().StringVar(&findKey, "match-key", "", "Show the data like the match key")
	cmd.Flags().StringVar(&findPrefix, "prefix", "", "Show the data like the prefix")
	cmd.Flags().StringVar(&findInput, "input", "", "KeyMeta JSONL file (offline mode); empty means online scan")
	cmd.Flags().BoolVar(&containValue, "value", false, "Show the value or not")
	cmd.Flags().IntVar(&findLimit, "limit", 10, "The limit of the show keys (pushed down to the etcd server as a Range limit)")
	return cmd
}

func findFunc(cmd *cobra.Command, args []string) {
	if findInput != "" {
		findFromJSONL()
		return
	}
	core.InitClient()
	var opts []core.ScanOption
	if findPrefix != "" {
		opts = append(opts, core.WithPrefix(findPrefix))
	}
	opts = append(opts, core.WithLimit(int64(findLimit)))
	resp, datac := core.ScanData(opts...)
	appendBufferForFind(resp, datac, os.Stdout)
}

// findFromJSONL searches a JSONL file for matching keys (offline mode).
func findFromJSONL() {
	metas, err := core.ReadJSONL(findInput)
	if err != nil {
		core.Exit(err)
	}
	var buffer bytes.Buffer
	buffer.WriteString("Kv List\n")
	buffer.WriteString("| Key | Value |\n")

	count := 0
	for _, m := range metas {
		if count >= findLimit {
			break
		}
		if findPrefix != "" && !strings.HasPrefix(m.Key, findPrefix) {
			continue
		}
		if findKey != "" && !strings.Contains(m.Key, findKey) {
			continue
		}
		v := ""
		if containValue && m.Value != nil {
			v = *m.Value
		}
		buffer.WriteString(fmt.Sprintf("| %s | %s |\n", m.Key, v))
		count++
	}
	buffer.WriteTo(os.Stdout)
}

func appendBufferForFind(resp *clientv3.GetResponse, datac <-chan []*mvccpb.KeyValue, writer io.Writer) {
	var buffer bytes.Buffer
	buffer.WriteString("Kv List\n")
	buffer.WriteString("| Key | Value |\n")

	count := 0
	for data := range datac {
		for _, kv := range data {
			if count >= findLimit {
				buffer.WriteTo(writer)
				return
			}
			key := string(kv.Key)
			if !strings.Contains(key, findKey) {
				continue
			}
			v := ""
			if containValue {
				v = base64.StdEncoding.EncodeToString(kv.Value)
			}
			buffer.WriteString(fmt.Sprintf("| %s | %s |\n",
				key, v))
			count++
		}
	}

	buffer.WriteTo(writer)
}
