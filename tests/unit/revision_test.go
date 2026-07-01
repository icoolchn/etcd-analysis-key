package unit

import (
	"testing"

	"github.com/SimFG/etcd-analysis/core"
)

func TestBytesToBucketKey_Normal(t *testing.T) {
	key := core.MakeRevisionKey(100, 1, false)
	bk, err := core.BytesToBucketKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if bk.Main != 100 || bk.Sub != 1 || bk.Tombstone {
		t.Errorf("got %+v, want Main=100 Sub=1 Tombstone=false", bk)
	}
}

func TestBytesToBucketKey_Tombstone(t *testing.T) {
	key := core.MakeRevisionKey(200, 0, true)
	bk, err := core.BytesToBucketKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if bk.Main != 200 || bk.Sub != 0 || !bk.Tombstone {
		t.Errorf("got %+v, want Main=200 Sub=0 Tombstone=true", bk)
	}
}

func TestBytesToBucketKey_RoundTrip(t *testing.T) {
	cases := []struct {
		main, sub int64
		tomb      bool
	}{
		{1, 0, false},
		{1, 0, true},
		{0, 0, false},
		{999999, 42, false},
		{999999, 42, true},
	}
	for _, c := range cases {
		key := core.MakeRevisionKey(c.main, c.sub, c.tomb)
		bk, err := core.BytesToBucketKey(key)
		if err != nil {
			t.Fatalf("MakeRevisionKey(%d,%d,%v) round-trip failed: %v", c.main, c.sub, c.tomb, err)
		}
		if bk.Main != c.main || bk.Sub != c.sub || bk.Tombstone != c.tomb {
			t.Errorf("MakeRevisionKey(%d,%d,%v) → %+v", c.main, c.sub, c.tomb, bk)
		}
	}
}

func TestBytesToBucketKey_InvalidLength(t *testing.T) {
	_, err := core.BytesToBucketKey([]byte("short"))
	if err == nil {
		t.Error("expected error for short key")
	}
}

func TestBytesToBucketKey_InvalidSeparator(t *testing.T) {
	key := core.MakeRevisionKey(1, 0, false)
	key[8] = 'X'
	_, err := core.BytesToBucketKey(key)
	if err == nil {
		t.Error("expected error for invalid separator")
	}
}
