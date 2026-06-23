package cmd

import (
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestCheckLookCombination(t *testing.T) {
	// Filter-only combination lives in core.CheckFilterCombination; the
	// --show-value conflict is look-specific and validated in validateLookFlags.
	cases := []struct {
		name     string
		keysOnly bool
		filter   string
		wantErr  bool
	}{
		{"keys-only none ok", true, "none", false},
		{"keys-only key ok", true, "key", false},
		{"keys-only value err", true, "value", true},
		{"keys-only kv err", true, "kv", true},
		{"full value filter ok", false, "value", false},
		{"full kv filter ok", false, "kv", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := core.CheckFilterCombination(c.keysOnly, c.filter)
			if c.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestFilterReject(t *testing.T) {
	// Reset package flags to a known state.
	origMin, origMax, origAttr := filterMin, filterMax, filterAttribute
	defer func() { filterMin, filterMax, filterAttribute = origMin, origMax, origAttr }()

	kv := &mvccpb.KeyValue{Key: []byte("keykey"), Value: []byte("val")}

	tests := []struct {
		name string
		attr string
		min  int
		max  int
		want bool // true = rejected
	}{
		{"none never rejects", "none", -1, -1, false},
		{"key in range", "key", 3, 10, false},
		{"key below min", "key", 10, -1, true},
		{"key above max", "key", -1, 3, true},
		{"value in range", "value", 1, 10, false},
		{"value above max", "value", -1, 1, true},
		{"kv below min", "kv", 100, -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterMin, filterMax, filterAttribute = tt.min, tt.max, tt.attr
			if got := filterReject(kv); got != tt.want {
				t.Errorf("filterReject(%s,min=%d,max=%d) = %v, want %v", tt.attr, tt.min, tt.max, got, tt.want)
			}
		})
	}
}
