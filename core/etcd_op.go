package core

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

var EtcdOpTimeout time.Duration

// lastStatus caches the most recent StatusResponse from InitClient's connectivity
// check. distribute's Overview reads it for current_revision / db_size without a
// second gRPC round-trip. nil when offline (no client) or before InitClient.
var lastStatus *clientv3.StatusResponse

func EtcdPut(etcdCli *clientv3.Client, key, val string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	_, err := etcdCli.Put(ctx, key, val, opts...)
	return err
}

func EtcdGet(etcdCli *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	resp, err := etcdCli.Get(ctx, key, opts...)
	return resp, err
}

func EtcdDelete(etcdCli *clientv3.Client, key string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	_, err := etcdCli.Delete(ctx, key, opts...)
	return err
}

func EtcdTxn(etcdCli *clientv3.Client, fun func(txn clientv3.Txn)) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	etcdTxn := etcdCli.Txn(ctx)
	fun(etcdTxn)
}

func EtcdStatus(etcdCli *clientv3.Client) error {
	resp, err := EtcdStatusResponse(etcdCli)
	if err != nil {
		return err
	}
	lastStatus = resp
	return nil
}

// EtcdStatusResponse queries the first reachable endpoint and returns its
// StatusResponse. The caller (distribute Overview) can read Header.Revision
// (current revision) and DbSize/DbSizeInUse from it. CompactRevision is NOT
// present in StatusResponse on v3.5.x — it only appears on WatchResponse — so
// the Overview deliberately omits compact-revision / revision-gap / compact-count.
func EtcdStatusResponse(etcdCli *clientv3.Client) (*clientv3.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	for _, endpoint := range etcdCli.Endpoints() {
		resp, err := etcdCli.Status(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	return nil, nil
}

// LastStatus returns the cached StatusResponse from the last InitClient check,
// or nil if offline / not yet initialized.
func LastStatus() *clientv3.StatusResponse { return lastStatus }
