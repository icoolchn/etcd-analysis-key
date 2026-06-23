package unit

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func intPtr(v int) *int    { return &v }
func strPtr(v string) *string { return &v }

func TestKVToMeta_FullMode(t *testing.T) {
	kv := &mvccpb.KeyValue{
		Key:            []byte("/registry/pods/default/nginx"),
		Value:          []byte("hello"),
		CreateRevision: 10,
		ModRevision:    20,
		Version:        3,
		Lease:          0,
	}
	m := core.KVToMeta(kv, false, false)
	if m.Key != "/registry/pods/default/nginx" {
		t.Errorf("key = %q", m.Key)
	}
	if m.KeySizeBytes != 28 {
		t.Errorf("key_size = %d, want 28", m.KeySizeBytes)
	}
	if m.ValueSizeBytes == nil || *m.ValueSizeBytes != 5 {
		t.Errorf("value_size = %v, want 5", m.ValueSizeBytes)
	}
	if m.KvSizeBytes == nil || *m.KvSizeBytes != 33 {
		t.Errorf("kv_size = %v, want 33", m.KvSizeBytes)
	}
	if m.Value == nil || *m.Value != "-" {
		t.Errorf("value placeholder = %v, want '-'", m.Value)
	}
	if !m.HasValue() {
		t.Error("HasValue should be true in full mode")
	}
}

func TestKVToMeta_KeysOnlyMode(t *testing.T) {
	kv := &mvccpb.KeyValue{
		Key:            []byte("/a/b"),
		Value:          []byte("ignored"),
		CreateRevision: 1,
		ModRevision:    2,
		Version:        1,
		Lease:          42,
	}
	m := core.KVToMeta(kv, true, false)
	if m.HasValue() {
		t.Error("HasValue should be false in keys-only mode")
	}
	if m.Value != nil {
		t.Errorf("value should be nil in keys-only mode, got %v", m.Value)
	}
	if m.ValueSizeBytes != nil || m.KvSizeBytes != nil {
		t.Error("size pointers should be nil in keys-only mode")
	}
	if m.KvSize() != -1 || m.ValueSize() != -1 {
		t.Errorf("unknown sizes should report -1")
	}
	if m.Lease != 42 {
		t.Errorf("lease = %d, want 42", m.Lease)
	}
}

func TestKVToMeta_ShowValueBase64(t *testing.T) {
	kv := &mvccpb.KeyValue{Key: []byte("k"), Value: []byte("hi")}
	m := core.KVToMeta(kv, false, true)
	if m.Value == nil || *m.Value != "aGk=" { // base64("hi")
		t.Errorf("show-value base64 = %v, want aGk=", m.Value)
	}
}

func TestKeyMeta_JSONL_RoundTrip(t *testing.T) {
	full := core.KVToMeta(&mvccpb.KeyValue{
		Key: []byte("/a/b"), Value: []byte("xyz"),
		CreateRevision: 5, ModRevision: 6, Version: 2, Lease: 9,
	}, false, false)
	keysOnly := core.KVToMeta(&mvccpb.KeyValue{
		Key: []byte("/c/d"),
		CreateRevision: 7, ModRevision: 8, Version: 1, Lease: 0,
	}, true, false)

	var buf bytes.Buffer
	if err := core.WriteJSONL([]core.KeyMeta{full, keysOnly}, &buf); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 jsonl lines, got %d", len(lines))
	}

	// Full line should include value/size fields. key "/a/b" (4) + value "xyz" (3) = 7.
	var fullParsed core.KeyMeta
	if err := json.Unmarshal([]byte(lines[0]), &fullParsed); err != nil {
		t.Fatal(err)
	}
	if fullParsed.KvSizeBytes == nil || *fullParsed.KvSizeBytes != 7 {
		t.Errorf("full kv_size = %v, want 7", fullParsed.KvSizeBytes)
	}
	if fullParsed.ValueSizeBytes == nil || *fullParsed.ValueSizeBytes != 3 {
		t.Errorf("full value_size = %v, want 3", fullParsed.ValueSizeBytes)
	}

	// Keys-only line should omit value/size fields.
	var koParsed core.KeyMeta
	if err := json.Unmarshal([]byte(lines[1]), &koParsed); err != nil {
		t.Fatal(err)
	}
	if koParsed.Value != nil || koParsed.ValueSizeBytes != nil || koParsed.KvSizeBytes != nil {
		t.Errorf("keys-only JSONL should omit value/size fields: %+v", koParsed)
	}
	if strings.Contains(lines[1], "value_size_bytes") {
		t.Errorf("keys-only JSONL must not contain value_size_bytes: %s", lines[1])
	}
}

func TestFormatLogLine_FullAndKeysOnly(t *testing.T) {
	full := core.KVToMeta(&mvccpb.KeyValue{Key: []byte("k"), Value: []byte("hello")}, false, false)
	fullLine := core.FormatLogLine(full)
	for _, want := range []string{"key=k", "value=-", "key_size_bytes=1", "value_size_bytes=5", "kv_size_bytes=6", "kv_size_human=", "create_revision="} {
		if !strings.Contains(fullLine, want) {
			t.Errorf("full log line missing %q: %s", want, fullLine)
		}
	}

	ko := core.KVToMeta(&mvccpb.KeyValue{Key: []byte("k")}, true, false)
	koLine := core.FormatLogLine(ko)
	if strings.Contains(koLine, "value=") {
		t.Errorf("keys-only log line should not contain value=: %s", koLine)
	}
	if strings.Contains(koLine, "kv_size_bytes=") {
		t.Errorf("keys-only log line should not contain kv_size_bytes=: %s", koLine)
	}
	if !strings.Contains(koLine, "key_size_bytes=1") {
		t.Errorf("keys-only log line missing key_size_bytes=1: %s", koLine)
	}
}
