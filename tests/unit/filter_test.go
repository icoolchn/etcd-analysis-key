package unit

import (
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestFilterConfig_Reject_KV(t *testing.T) {
	kv := &mvccpb.KeyValue{Key: []byte("keykey"), Value: []byte("val")} // key=6, value=3, kv=9
	cases := []struct {
		name string
		f    core.FilterConfig
		want bool // true = rejected
	}{
		{"none never rejects", core.FilterConfig{Attribute: "none"}, false},
		{"empty attr never rejects", core.FilterConfig{}, false},
		{"key in range", core.FilterConfig{Attribute: "key", Min: 3, Max: 10}, false},
		{"key below min", core.FilterConfig{Attribute: "key", Min: 10}, true},
		{"key above max", core.FilterConfig{Attribute: "key", Max: 3}, true},
		{"value in range", core.FilterConfig{Attribute: "value", Min: 1, Max: 10}, false},
		{"value above max", core.FilterConfig{Attribute: "value", Max: 1}, true},
		{"kv below min", core.FilterConfig{Attribute: "kv", Min: 100}, true},
		{"kv in range", core.FilterConfig{Attribute: "kv", Min: 9, Max: 9}, false},
		{"min<0 max<0 no filter", core.FilterConfig{Attribute: "key", Min: -1, Max: -1}, false},
		{"unknown attr never rejects", core.FilterConfig{Attribute: "nope", Min: 1}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.f.Reject(kv); got != c.want {
				t.Errorf("Reject(%+v) = %v, want %v", c.f, got, c.want)
			}
		})
	}
}

func TestFilterConfig_RejectMeta(t *testing.T) {
	// Full record: key="/a/b" (4), value "xyz" (3), kv 7.
	full := core.KVToMeta(&mvccpb.KeyValue{Key: []byte("/a/b"), Value: []byte("xyz")}, false, false)
	// Keys-only: no size pointers.
	ko := core.KVToMeta(&mvccpb.KeyValue{Key: []byte("/a/b")}, true, false)

	if (core.FilterConfig{Attribute: "value", Min: 1}).RejectMeta(ko) {
		t.Error("keys-only value filter should be no-op (nil size), not reject")
	}
	if (core.FilterConfig{Attribute: "kv", Min: 1}).RejectMeta(ko) {
		t.Error("keys-only kv filter should be no-op (nil size), not reject")
	}
	if !(core.FilterConfig{Attribute: "key", Min: 100}).RejectMeta(full) {
		t.Error("key=4 below min=100 should reject")
	}
	if (core.FilterConfig{Attribute: "kv", Min: 7, Max: 7}).RejectMeta(full) {
		t.Error("kv=7 in [7,7] should not reject")
	}
	if !(core.FilterConfig{Attribute: "value", Max: 2}).RejectMeta(full) {
		t.Error("value=3 above max=2 should reject")
	}
}

func TestFilterMetas(t *testing.T) {
	metas := []core.KeyMeta{
		core.KVToMeta(&mvccpb.KeyValue{Key: []byte("/a/short"), Value: []byte("v")}, false, false), // kv=8
		core.KVToMeta(&mvccpb.KeyValue{Key: []byte("/a/longerkey"), Value: []byte("vvvv")}, false, false), // kv=13
	}
	out := core.FilterMetas(metas, core.FilterConfig{Attribute: "kv", Min: 10})
	if len(out) != 1 {
		t.Fatalf("expected 1 after filter, got %d", len(out))
	}
	if out[0].Key != "/a/longerkey" {
		t.Errorf("kept wrong record: %s", out[0].Key)
	}
	// none filter returns input slice unchanged (same length, no copy semantics required).
	none := core.FilterMetas(metas, core.FilterConfig{Attribute: "none"})
	if len(none) != 2 {
		t.Errorf("none filter should keep all, got %d", len(none))
	}
}

func TestCheckFilterCombination(t *testing.T) {
	cases := []struct {
		keysOnly bool
		filter   string
		wantErr  bool
	}{
		{true, "none", false},
		{true, "key", false},
		{true, "value", true},
		{true, "kv", true},
		{false, "value", false},
		{false, "kv", false},
	}
	for _, c := range cases {
		err := core.CheckFilterCombination(c.keysOnly, c.filter)
		if c.wantErr && err == nil {
			t.Errorf("keysOnly=%v filter=%s: expected error", c.keysOnly, c.filter)
		}
		if !c.wantErr && err != nil {
			t.Errorf("keysOnly=%v filter=%s: unexpected error %v", c.keysOnly, c.filter, err)
		}
	}
}
