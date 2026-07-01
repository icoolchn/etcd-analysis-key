package unit

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/SimFG/etcd-analysis/core"
	"github.com/golang/protobuf/proto"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func createTestDB(t *testing.T, entries []struct {
	main, sub int64
	tomb      bool
	kv        *mvccpb.KeyValue
}) string {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		for _, e := range entries {
			revKey := core.MakeRevisionKey(e.main, e.sub, e.tomb)
			val, err := proto.Marshal(e.kv)
			if err != nil {
				return err
			}
			if err := b.Put(revKey, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return dbPath
}

func TestSnapshotSource_SingleKey(t *testing.T) {
	kv := &mvccpb.KeyValue{
		Key: []byte("/app/config"), Value: []byte("v1"),
		CreateRevision: 2, ModRevision: 2, Version: 1,
	}
	dbPath := createTestDB(t, []struct {
		main, sub int64
		tomb      bool
		kv        *mvccpb.KeyValue
	}{
		{2, 0, false, kv},
	})

	ch, stats, err := core.SnapshotSource(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	var results []*mvccpb.KeyValue
	for batch := range ch {
		results = append(results, batch...)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 KV, got %d", len(results))
	}
	if string(results[0].Key) != "/app/config" {
		t.Errorf("key = %q", string(results[0].Key))
	}

	s := stats["/app/config"]
	if s == nil {
		t.Fatal("stats missing for /app/config")
	}
	if s.RevCount != 1 || s.TombstoneCount != 0 {
		t.Errorf("stats = rev:%d tomb:%d, want rev:1 tomb:0", s.RevCount, s.TombstoneCount)
	}
}

func TestSnapshotSource_DedupMultipleRevisions(t *testing.T) {
	entries := []struct {
		main, sub int64
		tomb      bool
		kv        *mvccpb.KeyValue
	}{
		{2, 0, false, &mvccpb.KeyValue{Key: []byte("/a"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1}},
		{5, 0, false, &mvccpb.KeyValue{Key: []byte("/a"), Value: []byte("v2"), CreateRevision: 2, ModRevision: 5, Version: 2}},
		{8, 0, false, &mvccpb.KeyValue{Key: []byte("/a"), Value: []byte("v3"), CreateRevision: 2, ModRevision: 8, Version: 3}},
	}
	dbPath := createTestDB(t, entries)

	ch, stats, err := core.SnapshotSource(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var results []*mvccpb.KeyValue
	for batch := range ch {
		results = append(results, batch...)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 KV after dedup, got %d", len(results))
	}
	if string(results[0].Value) != "v3" {
		t.Errorf("expected latest value 'v3', got %q", string(results[0].Value))
	}

	s := stats["/a"]
	if s.RevCount != 3 {
		t.Errorf("RevCount = %d, want 3", s.RevCount)
	}
	if s.TombstoneCount != 0 {
		t.Errorf("TombstoneCount = %d, want 0", s.TombstoneCount)
	}
}

func TestSnapshotSource_TombstoneRemovesKey(t *testing.T) {
	entries := []struct {
		main, sub int64
		tomb      bool
		kv        *mvccpb.KeyValue
	}{
		{2, 0, false, &mvccpb.KeyValue{Key: []byte("/deleted"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1}},
		{5, 0, true, &mvccpb.KeyValue{Key: []byte("/deleted"), Value: nil, CreateRevision: 2, ModRevision: 5, Version: 2}},
	}
	dbPath := createTestDB(t, entries)

	ch, stats, err := core.SnapshotSource(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var results []*mvccpb.KeyValue
	for batch := range ch {
		results = append(results, batch...)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 KVs (key was deleted), got %d", len(results))
	}

	s := stats["/deleted"]
	if s == nil {
		t.Fatal("stats missing for /deleted")
	}
	if s.RevCount != 2 {
		t.Errorf("RevCount = %d, want 2", s.RevCount)
	}
	if s.TombstoneCount != 1 {
		t.Errorf("TombstoneCount = %d, want 1", s.TombstoneCount)
	}
}

func TestSnapshotSource_PrefixFilter(t *testing.T) {
	entries := []struct {
		main, sub int64
		tomb      bool
		kv        *mvccpb.KeyValue
	}{
		{2, 0, false, &mvccpb.KeyValue{Key: []byte("/app/a"), Value: []byte("1"), CreateRevision: 2, ModRevision: 2, Version: 1}},
		{3, 0, false, &mvccpb.KeyValue{Key: []byte("/app/b"), Value: []byte("2"), CreateRevision: 3, ModRevision: 3, Version: 1}},
		{4, 0, false, &mvccpb.KeyValue{Key: []byte("/other/c"), Value: []byte("3"), CreateRevision: 4, ModRevision: 4, Version: 1}},
	}
	dbPath := createTestDB(t, entries)

	ch, stats, err := core.SnapshotSource(dbPath, core.WithSnapshotPrefix("/app/"))
	if err != nil {
		t.Fatal(err)
	}
	var results []*mvccpb.KeyValue
	for batch := range ch {
		results = append(results, batch...)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 KVs with prefix /app/, got %d", len(results))
	}
	if _, ok := stats["/other/c"]; ok {
		t.Error("/other/c should be filtered out")
	}
}

func TestListBuckets(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"key", "meta", "alarm"} {
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	names, err := core.ListBuckets(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	if len(names) != 3 {
		t.Fatalf("expected 3 buckets, got %d: %v", len(names), names)
	}
	if names[0] != "alarm" || names[1] != "key" || names[2] != "meta" {
		t.Errorf("buckets = %v", names)
	}
}

func TestScanKeys_RevisionRange(t *testing.T) {
	entries := []struct {
		main, sub int64
		tomb      bool
		kv        *mvccpb.KeyValue
	}{
		{2, 0, false, &mvccpb.KeyValue{Key: []byte("/a"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1}},
		{5, 0, false, &mvccpb.KeyValue{Key: []byte("/a"), Value: []byte("v2"), CreateRevision: 2, ModRevision: 5, Version: 2}},
		{10, 0, false, &mvccpb.KeyValue{Key: []byte("/b"), Value: []byte("v1"), CreateRevision: 10, ModRevision: 10, Version: 1}},
	}
	dbPath := createTestDB(t, entries)

	keys, err := core.ScanKeys(dbPath, core.ScanKeysConfig{StartRevision: 3, EndRevision: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 key in range [3,8], got %d", len(keys))
	}
	if keys[0].Rev.Main != 5 {
		t.Errorf("expected main=5, got %d", keys[0].Rev.Main)
	}
}

func TestSnapshotSource_NonExistentFile(t *testing.T) {
	_, _, err := core.SnapshotSource("/nonexistent/path.db")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestSnapshotSource_EmptyDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "empty.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	ch, stats, err := core.SnapshotSource(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var results []*mvccpb.KeyValue
	for batch := range ch {
		results = append(results, batch...)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 KVs from empty db, got %d", len(results))
	}
	if len(stats) != 0 {
		t.Errorf("expected 0 stats from empty db, got %d", len(stats))
	}
}

func TestIterateBucket_WithLimit(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if err := b.Put([]byte{byte(i)}, []byte{byte(i + 100)}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	entries, err := core.IterateBucket(dbPath, "test", core.IterateBucketConfig{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries with limit, got %d", len(entries))
	}

	all, err := core.IterateBucket(dbPath, "test", core.IterateBucketConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 10 {
		t.Errorf("expected 10 entries without limit, got %d", len(all))
	}
}

func TestSnapshotKVToMeta_Integration(t *testing.T) {
	entries := []struct {
		main, sub int64
		tomb      bool
		kv        *mvccpb.KeyValue
	}{
		{2, 0, false, &mvccpb.KeyValue{Key: []byte("/x"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1}},
		{5, 0, false, &mvccpb.KeyValue{Key: []byte("/x"), Value: []byte("v2update"), CreateRevision: 2, ModRevision: 5, Version: 2}},
		{7, 0, true, &mvccpb.KeyValue{Key: []byte("/x"), Value: nil, CreateRevision: 2, ModRevision: 7, Version: 3}},
		{3, 0, false, &mvccpb.KeyValue{Key: []byte("/y"), Value: []byte("alive"), CreateRevision: 3, ModRevision: 3, Version: 1}},
	}
	dbPath := createTestDB(t, entries)

	ch, stats, err := core.SnapshotSource(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	var metas []core.KeyMeta
	for batch := range ch {
		for _, kv := range batch {
			key := string(kv.Key)
			s := stats[key]
			metas = append(metas, core.SnapshotKVToMeta(kv, s.RevCount, s.TombstoneCount))
		}
	}

	if len(metas) != 1 {
		t.Fatalf("expected 1 surviving key (/y), got %d", len(metas))
	}

	m := metas[0]
	if m.Key != "/y" {
		t.Errorf("key = %q, want /y", m.Key)
	}
	if m.RevCount == nil || *m.RevCount != 1 {
		t.Errorf("rev_count = %v, want 1", m.RevCount)
	}
	if m.TombstoneCount == nil || *m.TombstoneCount != 0 {
		t.Errorf("tombstone_count = %v, want 0", m.TombstoneCount)
	}

	xStats := stats["/x"]
	if xStats.RevCount != 3 || xStats.TombstoneCount != 1 {
		t.Errorf("/x stats = rev:%d tomb:%d, want rev:3 tomb:1", xStats.RevCount, xStats.TombstoneCount)
	}
}

func TestScanKeys_WithLimit(t *testing.T) {
	entries := []struct {
		main, sub int64
		tomb      bool
		kv        *mvccpb.KeyValue
	}{
		{2, 0, false, &mvccpb.KeyValue{Key: []byte("/a"), Value: []byte("1"), CreateRevision: 2, ModRevision: 2, Version: 1}},
		{3, 0, false, &mvccpb.KeyValue{Key: []byte("/b"), Value: []byte("2"), CreateRevision: 3, ModRevision: 3, Version: 1}},
		{4, 0, false, &mvccpb.KeyValue{Key: []byte("/c"), Value: []byte("3"), CreateRevision: 4, ModRevision: 4, Version: 1}},
	}
	dbPath := createTestDB(t, entries)

	keys, err := core.ScanKeys(dbPath, core.ScanKeysConfig{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 keys with limit, got %d", len(keys))
	}
}

func TestCreateTestDBAndScan(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("key"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("db file should exist")
	}

	names, err := core.ListBuckets(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "key" {
		t.Errorf("buckets = %v, want [key]", names)
	}
}
