package core

import (
	"encoding/binary"
	"fmt"
)

const (
	revBytesLen       = 8 + 1 + 8       // main(8) + '_'(1) + sub(8)
	markedRevBytesLen = revBytesLen + 1  // + tombstone marker
	markBytePosition  = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

// BucketKey represents the decoded form of a bbolt key in the "key" bucket.
// Layout: main(8 bytes big-endian) + '_' + sub(8 bytes big-endian) + optional 't'.
type BucketKey struct {
	Main      int64
	Sub       int64
	Tombstone bool
}

// BytesToBucketKey decodes a raw bbolt key from the "key" bucket into a
// BucketKey. Adapted from ahrtr/etcd-diagnosis offline/revision.go.
func BytesToBucketKey(b []byte) (BucketKey, error) {
	if len(b) != revBytesLen && len(b) != markedRevBytesLen {
		return BucketKey{}, fmt.Errorf("invalid revision key length: %d (want %d or %d)", len(b), revBytesLen, markedRevBytesLen)
	}
	if b[8] != '_' {
		return BucketKey{}, fmt.Errorf("invalid revision key separator: %q (want '_')", b[8])
	}
	return BucketKey{
		Main:      int64(binary.BigEndian.Uint64(b[0:8])),
		Sub:       int64(binary.BigEndian.Uint64(b[9:17])),
		Tombstone: len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone,
	}, nil
}

// MakeRevisionKey encodes a BucketKey back to its byte representation.
// Used in tests to create well-formed bbolt keys.
func MakeRevisionKey(main, sub int64, tombstone bool) []byte {
	n := revBytesLen
	if tombstone {
		n = markedRevBytesLen
	}
	b := make([]byte, n)
	binary.BigEndian.PutUint64(b[0:8], uint64(main))
	b[8] = '_'
	binary.BigEndian.PutUint64(b[9:17], uint64(sub))
	if tombstone {
		b[markBytePosition] = markTombstone
	}
	return b
}
