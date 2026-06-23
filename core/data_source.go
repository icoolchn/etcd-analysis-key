package core

import (
	"fmt"
	"math/rand"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	readCount = 1000
)

// DefaultPageSize returns the default per-request page size used by ScanData.
func DefaultPageSize() int { return readCount }

var client *clientv3.Client

// ScanOption configures a ScanData call.
type ScanOption func(*scanConfig)

type scanConfig struct {
	prefix    string
	keysOnly  bool
	pageSize  int
	pageSleep time.Duration
	limit     int64 // >0 means a single server-capped request (no pagination)
}

// WithPrefix restricts the scan to the given prefix. Pagination uses a
// prefix-bounded range so large prefixes are read in pages rather than in a
// single response.
func WithPrefix(prefix string) ScanOption {
	return func(c *scanConfig) { c.prefix = prefix }
}

// WithKeysOnly enables etcd's keys-only Range: value bodies are not
// transferred, but key/create_revision/mod_revision/version/lease metadata
// still are. Suitable for low-risk snapshots.
func WithKeysOnly() ScanOption {
	return func(c *scanConfig) { c.keysOnly = true }
}

// WithPageSize overrides the per-request page size (default readCount=1000).
// Larger pages reduce Range request count but raise single-response memory.
func WithPageSize(n int) ScanOption {
	return func(c *scanConfig) {
		if n > 0 {
			c.pageSize = n
		}
	}
}

// WithPageSleep pauses for d between pages, easing instantaneous follower load.
func WithPageSleep(d time.Duration) ScanOption {
	return func(c *scanConfig) { c.pageSleep = d }
}

// WithLimit pushes a server-side limit. When set (>0), ScanData issues a
// single Range request capped at n keys instead of paginating. This is the
// push-down used by `find --limit`.
func WithLimit(n int64) ScanOption {
	return func(c *scanConfig) {
		if n > 0 {
			c.limit = n
		}
	}
}

func InitClient() *clientv3.Client {
	connEndpoints := []string{"127.0.0.1:2379"}

	l := len(C.Endpoints)
	if l != 0 {
		connEndpoints = []string{C.Endpoints[rand.Intn(l)]}
	}

	cfg := clientv3.Config{
		Endpoints: connEndpoints,
	}

	if C.TLS.CertFile != "" && C.TLS.KeyFile != "" && C.TLS.TrustedCAFile != "" {
		tlsConfig, err := C.TLS.ClientConfig()
		if err != nil {
			Exit(fmt.Errorf("failed to get etcd tls config, err is %v", err))
		}
		cfg.TLS = tlsConfig
		cfg.TLS.InsecureSkipVerify = true
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		Exit(fmt.Errorf("dial error: %v\n", err))
	}
	EtcdOpTimeout = time.Duration(C.CommandTimeout) * time.Second
	err = EtcdStatus(c)
	if err != nil {
		Exit(fmt.Errorf("unvaliable etcd server, error: %v\n", err))
	}
	client = c
	return client
}

// ScanData streams etcd key-values according to the given options.
//
// Pagination semantics:
//   - With WithLimit(n) set, a single Range request capped at n keys is issued
//     and the channel is closed after the first response.
//   - Otherwise the keyspace (optionally prefix-bounded) is read in pages of
//     pageSize. Each page after the first re-requests the last key of the
//     previous page and the duplicate leading key is dropped, matching the
//     original from-key pagination behavior.
func ScanData(opts ...ScanOption) (*clientv3.GetResponse, <-chan []*mvccpb.KeyValue) {
	cfg := &scanConfig{pageSize: readCount}
	for _, o := range opts {
		o(cfg)
	}
	pageSize := int64(cfg.pageSize)

	c := make(chan []*mvccpb.KeyValue, 10)

	prefixEnd := ""
	if cfg.prefix != "" {
		prefixEnd = clientv3.GetPrefixRangeEnd(cfg.prefix)
	}

	buildOpts := func(count int64) []clientv3.OpOption {
		o := []clientv3.OpOption{clientv3.WithSerializable()}
		if cfg.keysOnly {
			o = append(o, clientv3.WithKeysOnly())
		}
		if count > 0 {
			o = append(o, clientv3.WithLimit(count))
		}
		return o
	}

	// Single server-capped request (e.g. find --limit). No pagination.
	if cfg.limit > 0 {
		var start string
		o := buildOpts(cfg.limit)
		if cfg.prefix != "" {
			start = cfg.prefix
			o = append(o, clientv3.WithRange(prefixEnd))
		} else {
			start = EmptyChar()
			o = append(o, clientv3.WithFromKey())
		}
		resp := EtcdGetOrFail(start, o...)
		c <- resp.Kvs
		close(c)
		return resp, c
	}

	// First page.
	var firstStart string
	firstOpts := buildOpts(pageSize)
	if cfg.prefix != "" {
		firstStart = cfg.prefix
		firstOpts = append(firstOpts, clientv3.WithRange(prefixEnd))
	} else {
		firstStart = EmptyChar()
		firstOpts = append(firstOpts, clientv3.WithFromKey())
	}
	resp := EtcdGetOrFail(firstStart, firstOpts...)
	c <- resp.Kvs

	go func(first *clientv3.GetResponse) {
		defer close(c)
		nextResp := first
		for {
			l := len(nextResp.Kvs)
			if int64(l) < pageSize {
				return
			}
			lastKey := nextResp.Kvs[l-1].Key
			o := buildOpts(pageSize)
			if cfg.prefix != "" {
				o = append(o, clientv3.WithRange(prefixEnd))
			} else {
				o = append(o, clientv3.WithFromKey())
			}
			nextResp = EtcdGetOrFail(string(lastKey), o...)
			nl := len(nextResp.Kvs)
			if nl <= 1 {
				return
			}
			c <- nextResp.Kvs[1:]
			if cfg.pageSleep > 0 {
				time.Sleep(cfg.pageSleep)
			}
		}
	}(resp)

	return resp, c
}

// EtcdGetOrFail wraps EtcdGet and exits on error, matching the original
// GetDataWithPrefix behavior where a get error is fatal.
func EtcdGetOrFail(key string, opts ...clientv3.OpOption) *clientv3.GetResponse {
	resp, err := EtcdGet(client, key, opts...)
	if err != nil {
		Exit(err)
	}
	return resp
}

// GetDataWithPrefix is retained for backward compatibility. New callers should
// use ScanData with WithPrefix.
func GetDataWithPrefix(prefix string) (*clientv3.GetResponse, <-chan []*mvccpb.KeyValue) {
	return ScanData(WithPrefix(prefix))
}

// GetAllData is retained for backward compatibility. New callers should use
// ScanData with no options.
func GetAllData() (*clientv3.GetResponse, <-chan []*mvccpb.KeyValue) {
	return ScanData()
}
