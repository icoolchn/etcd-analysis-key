package core

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// KeyMeta is the per-key metadata record used by `look --write-out=jsonl` and
// consumed by `summary --input`. Value and size fields are pointers so that
// keys-only records (which have no value) omit them cleanly from JSON.
type KeyMeta struct {
	Key            string  `json:"key"`
	Value          *string `json:"value,omitempty"`
	KeySizeBytes   int     `json:"key_size_bytes"`
	ValueSizeBytes *int    `json:"value_size_bytes,omitempty"`
	KvSizeBytes    *int    `json:"kv_size_bytes,omitempty"`
	CreateRevision int64   `json:"create_revision"`
	ModRevision    int64   `json:"mod_revision"`
	Version        int64   `json:"version"`
	Lease          int64   `json:"lease"`
}

// KVToMeta builds a KeyMeta from an etcd KeyValue. In keys-only mode value and
// size fields are left nil. showValue only takes effect in non-keys-only mode
// and base64-encodes the value body into Value; otherwise Value is the "-"
// placeholder.
func KVToMeta(kv *mvccpb.KeyValue, keysOnly bool, showValue bool) KeyMeta {
	m := KeyMeta{
		Key:            string(kv.Key),
		KeySizeBytes:   len(kv.Key),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Lease:          kv.Lease,
	}
	if keysOnly {
		return m
	}
	v := "-"
	if showValue {
		v = base64.StdEncoding.EncodeToString(kv.Value)
	}
	m.Value = &v
	vs := len(kv.Value)
	m.ValueSizeBytes = &vs
	ks := len(kv.Key) + len(kv.Value)
	m.KvSizeBytes = &ks
	return m
}

// HasValue reports whether size fields are populated (i.e. not keys-only).
func (m KeyMeta) HasValue() bool { return m.KvSizeBytes != nil }

// KvSize returns the kv size if known, else -1.
func (m KeyMeta) KvSize() int64 {
	if m.KvSizeBytes != nil {
		return int64(*m.KvSizeBytes)
	}
	return -1
}

// ValueSize returns the value size if known, else -1.
func (m KeyMeta) ValueSize() int64 {
	if m.ValueSizeBytes != nil {
		return int64(*m.ValueSizeBytes)
	}
	return -1
}

// WriteJSONL writes metas as one JSON object per line to w.
func WriteJSONL(metas []KeyMeta, w io.Writer) error {
	bw := bufio.NewWriter(w)
	for _, m := range metas {
		b, err := json.Marshal(m)
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

// ReadJSONL reads a JSONL file produced by `look --write-out=jsonl`.
func ReadJSONL(path string) ([]KeyMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var metas []KeyMeta
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var m KeyMeta
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return nil, fmt.Errorf("invalid jsonl at line %d: %w", lineNo, err)
		}
		metas = append(metas, m)
	}
	return metas, sc.Err()
}

// FormatLogLine renders a KeyMeta as the `look --write-out=log` single-line
// format with the split size fields. In keys-only mode value/value_size/
// kv_size are omitted.
func FormatLogLine(m KeyMeta) string {
	if m.HasValue() {
		return fmt.Sprintf("key=%s value=%s key_size_bytes=%d value_size_bytes=%d kv_size_bytes=%d kv_size_human=%s create_revision=%d mod_revision=%d version=%d lease=%d",
			m.Key, valStr(m.Value),
			m.KeySizeBytes, *m.ValueSizeBytes, *m.KvSizeBytes, ReadableSize(*m.KvSizeBytes),
			m.CreateRevision, m.ModRevision, m.Version, m.Lease)
	}
	return fmt.Sprintf("key=%s key_size_bytes=%d create_revision=%d mod_revision=%d version=%d lease=%d",
		m.Key, m.KeySizeBytes, m.CreateRevision, m.ModRevision, m.Version, m.Lease)
}

func valStr(v *string) string {
	if v == nil {
		return "-"
	}
	return *v
}
