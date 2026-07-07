# etcd-analysis-key

etcd is suited to small key-value pairs such as system metadata and service discovery, and is sensitive to value size: too many large values hurt watch stability, raise memory usage, and increase fsync pressure. This tool inspects etcd data distribution and large-key problems, with low-risk online scans and offline snapshot/WAL analysis.

* **Online**: keys-only scans with server-side prefix/limit pushdown and client-side aggregation, keeping production follower load low.
* **Offline**: parse a bbolt snapshot db or WAL directly without contacting the cluster; one JSONL export can be analyzed many times.
* **Safe**: read-only by default; high-risk commands (`clear`, `rename`) are disabled.

> Internal documentation: see the [docs/](docs/) directory.

## Getting started

### Build

```shell
$ go build -o etcdctl+
# Cross-compile for macOS (arm64/amd64) and linux amd64
$ make build-all
```

### Quick start

```shell
$ etcdctl+ --help
$ etcdctl+ distribute                                  # online data distribution overview
$ etcdctl+ summary --keys-only --group-depth=2 --sort=count --top=50
```

> **Note:** If you hit a panic like `` `WithPrefix` and `WithFromKey` cannot be set at the same time ``, upgrading the etcd client to v3.5.27 has fixed it. See [CHANGELOG.md](CHANGELOG.md).

## Commands

| Command | Description | Mode |
|---|---|---|
| `distribute` | Data distribution overview (overview + histograms + percentiles + diagnosis) | Online / `--input` offline |
| `look` | Export or view all KVs; supports keys-only, snapshot, JSONL | Online / `--snapshot` offline |
| `summary` | Aggregate keys by prefix into Top N, multi-dimension sort | Online / `--input` offline |
| `find` | Find keys by keyword or prefix | Online / `--input` offline |
| `wal-look` | Export WAL operations | Offline |
| `wal-summary` | Aggregate WAL Put/Delete counts by key | Offline |
| `dump` | Raw data export (`list-bucket` / `iterate-bucket` / `scan-keys` / `wal`) | Offline |
| `leader` | Get the leader node info | Online |
| `decode` | Base64-decode an etcd value | Local |
| `unmarshal` | Unmarshal a protobuf value via a `.proto` source file | Local |
| ~~`clear`~~ | Clear all etcd data *(disabled: high-risk, irreversible)* | — |
| ~~`rename`~~ | Rename an etcd key *(disabled: non-atomic, may cause inconsistency)* | — |

### distribute

Data distribution overview: scale metrics, size/version histograms with percentiles, top-prefix concentration, and a diagnosis summary.

```shell
$ etcdctl+ distribute --type=kv

=== Overview ===
  Total keys:                    5,103,962
  --- size (key) ---
  Total key size:                 48.5 MiB
  Avg key size:                  10B
  key size min / max:            8B / 1.2KiB
  key size p50 / p99:            10B / 96B
  --- size (value) ---
  Total value size:              2.9 GiB
  Avg value size:                621B
  value size min / max:          0B / 563.6KiB
  value size p50 / p99:          619B / 656B
  --- size (kv) ---
  Total kv size:                 ~3 GB
  Avg kv size:                   631B
  kv size min / max:             8B / 564.8KiB
  kv size p50 / p99:             629B / 656B
  --- activity ---
  Lease=0 (persistent):          2.60%
  create_revision min / max:     2 / 38,788,326,159
  mod_revision min / max:        5 / 38,788,328,082
  max version:                   1,146,241,278
  history_revisions (total):     online: unavailable
  tombstone_count (total):       online: unavailable

=== Size Distribution (kv) ===
  Size histogram:
    22.0B  [1]    |
    34.0B  [6]    |∎∎∎
    46.0B  [29]   |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    58.0B  [13]   |∎∎∎∎∎∎∎
    70.0B  [1]    |
    85.0B  [66]   |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
  Size distribution:
    10% in 38.0B
    25% in 39.0B
    50% in 76.0B
    75% in 83.0B
    90% in 85.0B
    99% in 656.0B

=== Version Distribution ===
  Version histogram:
    1       [5,033,382]  |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    2~10    [16,598]     |
    11~100  [53,972]     |
    101~1K  [8]          |
    1K~10K  [0]          |
    10K+    [2]          |
  Version distribution:
    10% in 1
    25% in 1
    50% in 1
    75% in 1
    90% in 1
    99% in 17

=== Count Concentration (top-5 depth-2 prefixes) ===
  /registry/events              5,049,978  (99.0%)
  /registry/persistentvolumes      21,336  (0.42%)
  /registry/pods                   12,381  (0.24%)
  /registry/leases                  3,830  (0.08%)
  /registry/services                3,769  (0.07%)

=== Diagnosis ===
  count:     ⚠️ CONCENTRATED (top-1 = 99.0% ≥ 80%)
  size:      ✅ OK (kv p99 = 656B)
  version:   ⚠️ WRITE HOTSPOT (max version = 1,146,241,278)
  lease:     ✅ OK (2.6% lease=0)
```

`--type` controls the Size Distribution basis (`kv` default / `key` / `value`); the Overview shows all three sizes regardless. Use `--write-out=json` for machine-readable output.

### look

Export or view all etcd data. Supports keys-only (no value transfer), snapshot db parsing, and JSONL export for offline analysis.

```shell
$ etcdctl+ look | more                                          # page through all KVs
$ etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl   # low-risk keys-only snapshot
$ etcdctl+ look --snapshot snapshot.db --write-out=jsonl --output=keys.jsonl  # offline full-field snapshot
$ etcdctl+ look --filter=key --filter-min=74 --filter-max=100       # keys in a size range
```

JSONL output (one KeyMeta per line; field format real, data is synthetic):

```json
{"key":"/registry/pods/default/nginx-deploy-abc","value":"-","key_size_bytes":44,"value_size_bytes":1024,"kv_size_bytes":1068,"create_revision":38762374190,"mod_revision":38762374190,"version":1,"lease":0,"rev_count":1,"tombstone_count":0}
```

`--snapshot` parses a bbolt snapshot db offline and outputs all fields (key/value sizes, `rev_count`, `tombstone_count`) in a single pass, no cluster connection needed. `--keys-only` is ignored with `--snapshot`.

### summary

Aggregate keys by prefix into Top N groups. Supports online scan and offline (`--input` reads a JSONL snapshot) modes.

```shell
# Online: keys-only, top prefixes by key count
$ etcdctl+ summary --keys-only --group-depth=2 --sort=count --top=50

# Offline: analyze a snapshot repeatedly without touching etcd
$ etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=max-version --top=50

# K8s <name>.<uid> keys aggregated by <name> (events/services/endpoints...)
$ etcdctl+ summary --input=keys.jsonl --prefix=/registry/events/kyuubi \
    --strip-suffix=. --group-depth=4 --sort=count --top=10

# Rank prefixes by lease diversity
$ etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=distinct-lease-count --top=10
```

Text output (field format real, data is synthetic):

```text
Summary: 12,000 keys, 3 groups by count

prefix              count    total_size  avg_size  max_size  max_version  latest_mod_revision  created_count  modified_count  key_leased_count  distinct_lease_count  max_lease             rev_count  tombstone_count  percent
/registry/events    10,000   5.9 MiB     629B      1.2KiB    1            38788328063          10,000         10,000          10,000            10,000                5821917203594880612   10,000     0                83.3%
/registry/pods       1,500   5.3 MiB     3.6 KiB   8.0 KiB   12           38788327547          1,500          1,500           0                 0                     0                     1,500      0                12.5%
/registry/leases       500   150 KiB     300B      350B      4            38788328038          500            500             500               1                     5821917203580688138   500        0                4.2%
```

Key flags:

| Flag | Default | Description |
|---|---|---|
| `--group-depth` | `2` | Group by the first N path segments |
| `--strip-suffix` | off | Strip the trailing `<sep><suffix>` from the last path segment before grouping (e.g. `.` drops `.<uid>`) |
| `--top` | `20` | Output the top N groups |
| `--sort` | `count` | `count`, `total-size`, `avg-size`, `max-size`, `max-version`, `latest-mod-revision`, `created-count`, `modified-count`, `distinct-lease-count`, `rev-count`, `tombstone-count` |
| `--min/max-create-revision` | `0` | Bounds for `created-count` |
| `--min/max-mod-revision` | `0` | Bounds for `modified-count` |
| `--input` | off | JSONL snapshot file (offline mode); empty = online scan |
| `--keys-only` | `false` | Online: fetch key metadata only |
| `--prefix` | off | Online: server-side prefix scan |
| `--write-out` | `text` | `text` / `json` |

Lease columns (always shown; consistent with `distribute`'s `lease==0 = persistent`):

| Column | Description |
|---|---|
| `key_leased_count` | Keys with a non-zero lease (additive; summed in the `others` row) |
| `distinct_lease_count` | Number of distinct lease ids (non-additive; `-` in `others`) |
| `max_lease` | Largest lease id, a real `etcdctl lease inspect`-able id (non-additive; `-` in `others`) |

`key_leased_count / distinct_lease_count` is the lease-reuse factor: ≈1 → each key has its own lease; >>1 → few leases shared by many keys.

> **Production workflow:** For large clusters, export once with `look --keys-only --write-out=jsonl --output=keys.jsonl`, then analyze the JSONL offline with `summary --input=keys.jsonl` to avoid re-scanning etcd each time.

### find

Find keys by keyword (`--match-key`) or prefix (`--prefix`).

> **Note:** The `--key` flag was renamed to `--match-key` to avoid colliding with the global TLS `--key` flag.

```shell
$ etcdctl+ find --match-key=index --limit=10
```

### wal-look

Export WAL operations from an etcd data directory.

```shell
$ etcdctl+ wal-look --data-dir /var/lib/etcd \
    --start-index 1000 --end-index 2000 \
    --write-out=jsonl --output=wal.jsonl
```

JSONL output (one WalOp per line; field format real, data is synthetic). Operations like `Compaction` carry no `key`/`value_size_bytes`:

```json
{"raft_index":1500,"raft_term":3,"op_type":"Put","key":"/registry/pods/default/nginx-deploy-abc","value_size_bytes":1024,"entry_type":"IRRPut"}
{"raft_index":1501,"raft_term":3,"op_type":"Compaction","entry_type":"IRRCompaction"}
{"raft_index":1502,"raft_term":3,"op_type":"Delete","key":"/registry/pods/default/nginx-deploy-def","value_size_bytes":0,"entry_type":"IRRDeleteRange"}
```

`--entry-type` filters by entry type (comma-separated, e.g. `IRRPut,IRRDeleteRange`).

### wal-summary

Aggregate WAL Put/Delete counts per key, from a WalOp JSONL or by parsing WAL directly.

```shell
$ etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --top=50
$ etcdctl+ wal-summary --data-dir /var/lib/etcd --sort=delete-count --top=20
```

Text output (field format real, data is synthetic):

```text
WAL Summary: 8,000 total ops, 3,500 unique keys (top 10 by put-count)

key                                              put_count  delete_count  total_ops
/registry/pods/default/nginx-deploy-abc          2,500            50       2,550
/registry/pods/default/nginx-deploy-def          1,200             0       1,200
/registry/events/default/job-run-xyz             1,000           900       1,900
...
```

`--sort`: `put-count` (default) / `delete-count` / `total-ops`.

### dump

Raw data export from a snapshot db or WAL.

```shell
$ etcdctl+ dump list-bucket --snapshot snapshot.db
$ etcdctl+ dump iterate-bucket --snapshot snapshot.db --decode --limit 100
$ etcdctl+ dump scan-keys --snapshot snapshot.db --start-revision 100 --end-revision 200 --limit 50
$ etcdctl+ dump wal --data-dir /var/lib/etcd --start-index 1000 --end-index 2000
```

### leader

```shell
$ etcdctl+ leader
Name: default
ClientUrls: [http://127.0.0.1:2379]
```

### decode

Base64-decode an etcd value.

```shell
$ etcdctl+ decode --value=<base64-value>
```

### unmarshal

Unmarshal a protobuf value via a `.proto` source file, without compiling Go code.

> **Note:** The `--key` flag was renamed to `--target-key` to avoid colliding with the global TLS `--key` flag.

```shell
$ etcdctl+ unmarshal --target-key by-dev/meta/channelwatch/4/by-dev-rootcoord-dml_0_445337303926193462v0 \
    --import-path ../birdwatcher/proto/v2.2 \
    --proto ../birdwatcher/proto/v2.2/data_coord.proto \
    --full-message-name milvus.protov2.data.ChannelWatchInfo
```

## Documentation

- [Command cheatsheet](docs/etcd-analysis-key-commands.md)
- [Changelog](CHANGELOG.md)

## License

etcd-analysis-key is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
