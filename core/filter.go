package core

import (
	"fmt"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// FilterConfig describes a size-based filter shared by `look` and `summary`.
// Attribute is "none"|"key"|"value"|"kv"; Min/Max < 0 means that side is
// unbounded.
type FilterConfig struct {
	Attribute string
	Min       int
	Max       int
}

// Reject reports whether kv should be dropped by the filter rules. Used by the
// online scan path where the raw *mvccpb.KeyValue is available.
//
// A bound <= 0 means unbounded on that side, matching the CLI default of -1
// and making the zero-value FilterConfig a no-op.
func (f FilterConfig) Reject(kv *mvccpb.KeyValue) bool {
	if f.Attribute == "" || f.Attribute == "none" {
		return false
	}
	if f.Min <= 0 && f.Max <= 0 {
		return false
	}
	var size int
	switch f.Attribute {
	case "key":
		size = len(kv.Key)
	case "value":
		size = len(kv.Value)
	case "kv":
		size = len(kv.Key) + len(kv.Value)
	default:
		return false
	}
	return outOfRange(size, f.Min, f.Max)
}

// RejectMeta reports whether an offline KeyMeta record should be dropped.
// value/kv filters are no-ops on keys-only records (nil size pointers); the
// combination itself is rejected upstream by CheckFilterCombination.
func (f FilterConfig) RejectMeta(m KeyMeta) bool {
	if f.Attribute == "" || f.Attribute == "none" {
		return false
	}
	if f.Min <= 0 && f.Max <= 0 {
		return false
	}
	var size int
	switch f.Attribute {
	case "key":
		size = m.KeySizeBytes
	case "value":
		if m.ValueSizeBytes == nil {
			return false
		}
		size = *m.ValueSizeBytes
	case "kv":
		if m.KvSizeBytes == nil {
			return false
		}
		size = *m.KvSizeBytes
	default:
		return false
	}
	return outOfRange(size, f.Min, f.Max)
}

// FilterMetas returns a new slice with rejected records dropped, preserving
// order. The input slice is not mutated.
func FilterMetas(metas []KeyMeta, f FilterConfig) []KeyMeta {
	if f.Attribute == "" || f.Attribute == "none" {
		return metas
	}
	if f.Min <= 0 && f.Max <= 0 {
		return metas
	}
	out := make([]KeyMeta, 0, len(metas))
	for _, m := range metas {
		if !f.RejectMeta(m) {
			out = append(out, m)
		}
	}
	return out
}

// CheckFilterCombination enforces the --keys-only + --filter rules shared by
// `look` and `summary`. keys-only carries no value, so --filter=value|kv is
// meaningless and rejected.
func CheckFilterCombination(keysOnly bool, filter string) error {
	if !keysOnly {
		return nil
	}
	if filter == "value" || filter == "kv" {
		return fmt.Errorf("--keys-only cannot be combined with --filter=%s: keys-only has no value to size", filter)
	}
	return nil
}

// outOfRange reports whether size falls outside [min, max], treating <=0 bounds
// as unbounded on that side.
func outOfRange(size, min, max int) bool {
	if min > 0 && size < min {
		return true
	}
	if max > 0 && size > max {
		return true
	}
	return false
}
