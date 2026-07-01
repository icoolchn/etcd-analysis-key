package core

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// KeyStats holds per-key aggregation results from the snapshot traversal.
type KeyStats struct {
	KV             *mvccpb.KeyValue
	RevCount       int
	TombstoneCount int
}

// SnapshotOption configures a SnapshotSource call.
type SnapshotOption func(*snapshotConfig)

type snapshotConfig struct {
	prefix string
}

// WithSnapshotPrefix restricts the snapshot scan to keys with the given prefix.
func WithSnapshotPrefix(prefix string) SnapshotOption {
	return func(c *snapshotConfig) { c.prefix = prefix }
}

// SnapshotSource opens a bbolt snapshot db file read-only and performs a
// single-pass traversal of the "key" bucket. It simultaneously deduplicates to
// current-state KVs and aggregates rev_count/tombstone_count per key.
//
// Returns a channel of KV batches (compatible with the online ScanData
// pipeline) and a map of per-key stats for rev_count/tombstone_count lookup.
func SnapshotSource(dbPath string, opts ...SnapshotOption) (<-chan []*mvccpb.KeyValue, map[string]*KeyStats, error) {
	cfg := &snapshotConfig{}
	for _, o := range opts {
		o(cfg)
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("open snapshot db: %w", err)
	}

	stats := make(map[string]*KeyStats)
	c := make(chan []*mvccpb.KeyValue, 10)

	go func() {
		defer close(c)
		defer db.Close()

		_ = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("key"))
			if b == nil {
				return nil
			}
			cur := b.Cursor()
			for k, v := cur.First(); k != nil; k, v = cur.Next() {
				rev, err := BytesToBucketKey(k)
				if err != nil {
					continue
				}
				kv := &mvccpb.KeyValue{}
				if err := proto.Unmarshal(v, kv); err != nil {
					continue
				}
				key := string(kv.Key)
				if cfg.prefix != "" && len(key) < len(cfg.prefix) {
					continue
				}
				if cfg.prefix != "" && key[:len(cfg.prefix)] != cfg.prefix {
					continue
				}

				s, ok := stats[key]
				if !ok {
					s = &KeyStats{}
					stats[key] = s
				}
				s.RevCount++
				if rev.Tombstone {
					s.TombstoneCount++
					s.KV = nil
				} else {
					s.KV = kv
				}
			}

			batch := make([]*mvccpb.KeyValue, 0, 1000)
			for _, s := range stats {
				if s.KV == nil {
					continue
				}
				batch = append(batch, s.KV)
				if len(batch) >= 1000 {
					c <- batch
					batch = make([]*mvccpb.KeyValue, 0, 1000)
				}
			}
			if len(batch) > 0 {
				c <- batch
			}
			return nil
		})
	}()

	return c, stats, nil
}

// ListBuckets returns the names of all top-level buckets in the snapshot db.
func ListBuckets(dbPath string) ([]string, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open snapshot db: %w", err)
	}
	defer db.Close()

	var names []string
	_ = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			names = append(names, string(name))
			return nil
		})
	})
	return names, nil
}

// BucketEntry represents a single key-value pair from a bbolt bucket.
type BucketEntry struct {
	Key           []byte
	Value         []byte
	KeySizeBytes  int
	ValueSizeBytes int
}

// IterateBucketConfig controls IterateBucket behavior.
type IterateBucketConfig struct {
	Limit  int
	Decode bool
}

// IterateBucket iterates all entries in the named bucket, returning raw
// key/value pairs with size information.
func IterateBucket(dbPath, bucketName string, cfg IterateBucketConfig) ([]BucketEntry, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open snapshot db: %w", err)
	}
	defer db.Close()

	var entries []BucketEntry
	_ = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %q not found", bucketName)
		}
		cur := b.Cursor()
		count := 0
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			if cfg.Limit > 0 && count >= cfg.Limit {
				break
			}
			entry := BucketEntry{
				Key:            append([]byte(nil), k...),
				Value:          append([]byte(nil), v...),
				KeySizeBytes:   len(k),
				ValueSizeBytes: len(v),
			}
			entries = append(entries, entry)
			count++
		}
		return nil
	})
	return entries, nil
}

// ScanKeysConfig controls ScanKeys behavior.
type ScanKeysConfig struct {
	StartRevision int64
	EndRevision   int64
	Limit         int
}

// ScanKeys scans the "key" bucket by revision range, returning decoded
// BucketKey + mvccpb.KeyValue pairs.
type ScannedKey struct {
	Rev BucketKey
	KV  *mvccpb.KeyValue
}

// ScanKeys scans the "key" bucket and returns entries whose main revision
// falls within [start, end]. A zero bound means unbounded on that side.
func ScanKeys(dbPath string, cfg ScanKeysConfig) ([]ScannedKey, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open snapshot db: %w", err)
	}
	defer db.Close()

	var keys []ScannedKey
	_ = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("key"))
		if b == nil {
			return nil
		}
		cur := b.Cursor()
		count := 0
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			if cfg.Limit > 0 && count >= cfg.Limit {
				break
			}
			rev, err := BytesToBucketKey(k)
			if err != nil {
				continue
			}
			if cfg.StartRevision > 0 && rev.Main < cfg.StartRevision {
				continue
			}
			if cfg.EndRevision > 0 && rev.Main > cfg.EndRevision {
				continue
			}
			kv := &mvccpb.KeyValue{}
			if err := proto.Unmarshal(v, kv); err != nil {
				continue
			}
			keys = append(keys, ScannedKey{Rev: rev, KV: kv})
			count++
		}
		return nil
	})
	return keys, nil
}
