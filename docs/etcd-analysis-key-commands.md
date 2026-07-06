# etcd-analysis Command Cheatsheet

## Command Overview

| Command | Purpose | Type | Flag | Values |
|---------|---------|------|------|--------|
| **distribute** | Data size distribution | Read-only | `--type` | `key` (default), `value`, `kv` |
| | | | `--bucket` | int (default `5`) |
| | | | `--write-out` | `text` (default), `json` |
| | | | `--prefix` | string (server-side prefix scan) |
| | | | `--page-size` | int (default `1000`, keys per page) |
| | | | `--page-sleep` | duration (default `0`, e.g. `50ms`) |
| **look** | View/export all data | Read-only | `--show-value` | `true`, `false` (default) |
| | | | `--write-out` | `stdout` (default), **`jsonl` (recommended, structured)**, `file`, `log` (log ingestion, not for offline analysis) |
| | | | `--output` | file path (for `file`/`log`/`jsonl`) |
| | | | `--snapshot` | snapshot db file (offline single-pass all fields, includes rev_count/tombstone_count) |
| | | | `--hang` | `true`, `false` (default) |
| | | | `--hang-interval` | int, seconds (default `2`) |
| | | | `--filter` | `none` (default), `key`, `value`, `kv` (client-side filter) |
| | | | `--filter-min` | int, bytes (default `-1` = unbounded) |
| | | | `--filter-max` | int, bytes (default `-1` = unbounded) |
| | | | `--keys-only` | `true`, `false` (default); fetch key metadata only, no value |
| | | | `--prefix` | string (server-side prefix scan) |
| | | | `--page-size` | int (default `1000`) |
| | | | `--page-sleep` | duration (default `0`, e.g. `50ms`) |
| **summary** | Aggregate Top N by prefix | Read-only | `--input` | JSONL snapshot file (offline; empty = online scan) |
| | | | `--keys-only` | `true`, `false` (default, online only, no value) |
| | | | `--prefix` | string (online only, server-side prefix) |
| | | | `--group-depth` | int (default `2`, group by first N path segments) |
| | | | `--strip-suffix` | string (default empty = off; e.g. `.` strips trailing `.<uid>` from last segment before grouping) |
| | | | `--top` | int (default `20`) |
| | | | `--sort` | `count` (default), `total-size`, `avg-size`, `max-size`, `max-version`, `latest-mod-revision`, `created-count`, `modified-count`, `distinct-lease-count`, `rev-count`, `tombstone-count` |
| | | | `--min-create-revision` / `--max-create-revision` | int64 (default `0` = unbounded) |
| | | | `--min-mod-revision` / `--max-mod-revision` | int64 (default `0` = unbounded) |
| | | | `--filter` / `--filter-min` / `--filter-max` | client-side filter (default `none` / `-1` / `-1`) |
| | | | `--page-size` / `--page-sleep` | online only |
| | | | `--write-out` | `text` (default), `json` |
| | | | `--output` | file path (default stdout) |
| **find** | Search keys by keyword | Read-only | `--match-key` | string (fuzzy match) |
| | | | `--prefix` | string (key prefix) |
| | | | `--value` | `true`, `false` (default) |
| | | | `--limit` | int (default `10`, pushed down to etcd server as Range limit) |
| **wal-look** | Export WAL operation stream | Offline read-only | `--data-dir` | etcd data dir (with `member/wal`) or WAL dir (with `*.wal`) (required) |
| | | | `--write-out` | `stdout` (default), **`jsonl` (recommended)**, `log` (log ingestion) |
| | | | `--output` | file path |
| | | | `--start-index` | uint64 (default `0`, inclusive) |
| | | | `--end-index` | uint64 (default max, exclusive) |
| | | | `--entry-type` | comma-separated, 17 types (`IRRPut,IRRDeleteRange,...`; `-h` lists all) |
| **wal-summary** | Aggregate WAL writes by key | Offline read-only | `--input` | WalOp JSONL file (offline); empty = parse WAL directly |
| | | | `--data-dir` | etcd data dir or WAL dir (required when `--input` is empty) |
| | | | `--sort` | `put-count` (default), `delete-count`, `total-ops` |
| | | | `--top` | int (default `20`) |
| | | | `--write-out` | `text` (default), `json` |
| | | | `--output` | file path (default stdout) |
| **dump** | Raw data export | Offline read-only | subcommand | `list-bucket`, `iterate-bucket`, `scan-keys`, `wal` |
| | | | `--snapshot` | snapshot db path (required for db subcommands) |
| | | | `--data-dir` | etcd data dir or WAL dir (required for `wal` subcommand) |
| | | | `--start-index` / `--end-index` | raft index range (`wal` subcommand) |
| | | | `--entry-type` | comma-separated, 17 types (`wal` subcommand) |
| | | | `--start-revision` / `--end-revision` | revision range (`scan-keys`) |
| | | | `--decode` / `--limit` | decode as KeyValue / cap entries (`iterate-bucket`) |
| **unmarshal** | Protobuf unmarshal | Read-only | `--target-key` | string (full etcd key) |
| | | | `--import-path` | string, repeatable |
| | | | `--proto` | string (.proto file path), repeatable |
| | | | `--full-message-name` | string (`package.MessageName`) |
| **leader** | Query leader node | Read-only | none | — |
| **decode** | Base64 decode | Local only | `--value` | string (base64-encoded value) |
| **clear** | Clear all data | 🔴 Write (disabled) | none | — |
| **rename** | Rename a key | 🟠 Write (disabled) | `--source-key` | string (original key) |
| | | | `--target-key` | string (new key) |
| | | | `--bak` | `true` (default), `false` |
| **completion** | Shell completion | Local only | subcommand | `bash`, `zsh`, `fish`, `powershell` |

## Global Flags (shared by all commands)

| Flag | Default | Values | Description |
|------|---------|--------|-------------|
| `--endpoints` | `127.0.0.1:2379` | comma-separated addresses | etcd cluster address; for production troubleshooting, pass a single follower |
| `--cert` | empty | file path | TLS client cert file |
| `--key` | empty | file path | TLS client key file |
| `--cacert` | empty | file path | TLS CA file |
| `--command-timeout` | `5` | int, seconds | operation timeout |

> ⚠️ **TLS verification status:** Currently, configuring any of `--cert`/`--key`/`--cacert` unconditionally sets `InsecureSkipVerify=true` (skips server cert verification). Works for self-signed certs but has MITM risk. A future explicit `--insecure-skip-tls-verify` flag (strict by default) is planned but not yet implemented.

## Examples

### distribute

```bash
# Distribution by key size (default, text output)
etcdctl+ distribute

# By value size, 8 buckets
etcdctl+ distribute --type=value --bucket=8

# By key+value combined size
etcdctl+ distribute --type=kv

# Scan a prefix only (server-side, not full scan)
etcdctl+ distribute --prefix=/registry/events --type=value

# JSON output (structured, for programs / jq; distribute supports text/json only)
etcdctl+ distribute --type=value --write-out=json
etcdctl+ distribute --type=value --write-out=json | jq '.summary.largest_bytes'

# JSON to file for program consumption (distribute has no jsonl mode; json is the structured output)
etcdctl+ distribute --type=kv --write-out=json --output=distribute.json

# Production-safe scan: larger page, sleep between pages
etcdctl+ distribute --type=kv --page-size=5000 --page-sleep=50ms
```

### look

```bash
# Export all data as JSONL (recommended: best performance, structured, consumable by summary/find/distribute offline)
etcdctl+ look --write-out=jsonl --output=keys.jsonl

# keys-only JSONL snapshot: fetch key metadata only, no value transfer — lowest risk
etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl

# Full-field JSONL for a prefix (includes value size, not value content)
etcdctl+ look --prefix=/registry/events --write-out=jsonl --output=events-kv-meta.jsonl

# Offline snapshot db, single-pass all fields (rev_count/tombstone_count, no etcd connection)
etcdctl+ look --snapshot=snapshot.db --write-out=jsonl --output=keys.jsonl

# Terminal view (default stdout, no file; prefer jsonl to disk for large datasets)
etcdctl+ look
etcdctl+ look --show-value                  # show value (base64)
etcdctl+ look | more                        # pipe to pager

# Watch with 5s refresh (file mode only)
etcdctl+ look --write-out=file --hang=true --hang-interval=5

# Client-side filter: key size in 74~100 bytes (still fetches value; does not reduce server load)
etcdctl+ look --filter=key --filter-min=74 --filter-max=100

# Other output formats (not preferred for offline analysis):
#   --write-out=log   log format, for ingestion by loki etc.; unstructured, not consumable by summary --input
#   --write-out=file  plain text to file; for structured analysis use jsonl
etcdctl+ look --keys-only --write-out=log --output=keys.log

# Pipe
etcdctl+ look | more
```

> ⚠️ **Performance note:** look reads all etcd data (including value) by default. For production, prefer `--keys-only --write-out=jsonl` (no value transfer + structured to disk), then analyze offline repeatedly with `summary --input=keys.jsonl`; use `--prefix` to bound scope; fetch value only during off-peak when necessary. `--filter` is client-side and does not reduce server load. `--write-out=log/file` is for log/text scenarios — unstructured, not consumable by offline commands; use `jsonl` for analysis.

**look log/jsonl output fields (after splitting size):**

```text
# Full mode (with value)
key=... value=- key_size_bytes=33 value_size_bytes=10240 kv_size_bytes=10273 kv_size_human=10.1KiB create_revision=... mod_revision=... version=... lease=...

# keys-only mode (value-related fields omitted)
key=... key_size_bytes=33 create_revision=... mod_revision=... version=... lease=...
```

`--keys-only` combination rules: `--keys-only --filter=value|kv` is disallowed (no value to compute); `--keys-only --show-value` is disallowed (semantic conflict).

### summary

Aggregate keys by prefix into Top N groups. Supports online scan and offline (`--input` reads a JSONL snapshot) modes.

```bash
# Online convenience: keys-only, top prefixes by key count
etcdctl+ summary --keys-only --group-depth=2 --sort=count --top=50

# Offline: analyze a keys-only snapshot repeatedly without touching etcd
etcdctl+ summary --input=keys.jsonl --group-depth=2 --sort=count --top=50

# High-frequency overwrite hotspots (highest version)
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=max-version --top=50

# Most-modified prefixes after a point (revision from Grafana)
etcdctl+ summary --input=keys.jsonl --min-mod-revision=38770000000 --group-depth=3 --sort=count --top=50

# Most-created prefixes after a point
etcdctl+ summary --input=keys.jsonl --min-create-revision=38770000000 --group-depth=3 --sort=count --top=50

# Space usage of suspicious prefixes (needs a snapshot with value size)
etcdctl+ summary --input=events-kv-meta.jsonl --group-depth=3 --sort=total-size --top=50

# Online with client-side filter + safe scan
etcdctl+ summary --keys-only --filter=key --filter-min=100 --page-size=1000 --page-sleep=50ms

# JSON output for programs (summary supports text/json only, no jsonl mode)
etcdctl+ summary --input=keys.jsonl --sort=count --write-out=json --output=summary.json

# K8s <name>.<uid> keys aggregated by <name> (events/services/endpoints...)
# --strip-suffix=. strips the trailing .<uid> from the last segment; middle segments (e.g. monitoring.coreos.com) are untouched
etcdctl+ summary --input=keys.jsonl --prefix=/registry/events/kyuubi \
    --strip-suffix=. --group-depth=4 --sort=count --top=10

# Rank by lease diversity: which prefixes carry the most varied leases
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=distinct-lease-count --top=10
```

**`--group-depth` grouping** (example: `/registry/pods/default/nginx`):

| depth | group prefix |
|---|---|
| 1 | `/registry` |
| 2 | `/registry/pods` |
| 3 | `/registry/pods/default` |
| 4 | `/registry/pods/default/nginx` |

K8s guidance: `--group-depth=2` for resource type, `--group-depth=3` for resource type + namespace.

**`--sort` values:**

| sort | purpose |
|---|---|
| `count` | prefixes with the most keys |
| `total-size` / `avg-size` / `max-size` | largest total / avg object / single object (needs value-bearing snapshot) |
| `max-version` | high-frequency overwrite hotspots |
| `latest-mod-revision` | most recently active prefixes |
| `created-count` / `modified-count` | with `--min-*-revision`, prefixes most created/modified after a revision |
| `distinct-lease-count` | prefixes with the most distinct lease ids (most varied leases) |
| `rev-count` | prefixes with the most historical revisions (offline snapshot JSONL only) |
| `tombstone-count` | prefixes with the most tombstones (offline snapshot JSONL only) |

**Lease columns** (always shown; consistent with `distribute`'s `lease==0 = persistent`):

| column | meaning |
|---|---|
| `key_leased_count` | keys with a non-zero lease in the group (additive; summed in `others` row) |
| `distinct_lease_count` | number of distinct lease ids (non-additive; `-` in `others`) |
| `max_lease` | largest lease id, a real `etcdctl lease inspect`-able id (non-additive; `-` in `others`) |

`key_leased_count / distinct_lease_count` is the lease-reuse factor: ≈1 → each key has its own lease; >>1 → few leases shared by many keys.

**Text output example:**

```text
Summary: 2090000 keys, 15 groups (top 15 by count)

prefix | count | total_size | avg_size | max_size | max_version | latest_mod_revision | created_count | modified_count | key_leased_count | distinct_lease_count | max_lease | rev_count | tombstone_count | percent
/registry/events | 980000 | - | - | - | 3 | 38780000020 | 0 | 0 | 980000 | 980000 | 5821917203594880612 | 980000 | 0 | 99.0%
/registry/pods | 530000 | - | - | - | 1024 | 38780000010 | 0 | 0 | 0 | 0 | 0 | 530000 | 0 | 0.5%
...
```

> keys-only snapshots have no value size, so `total_size`/`avg_size`/`max_size` show `-`; `rev_count`/`tombstone_count` appear only in offline snapshot JSONL (online / plain keys-only snapshots omit these columns).

keys-only snapshots have no value size, so `TotalSize`/`AvgSize`/`MaxSize` show `-`; use `look --prefix=... --write-out=jsonl` (without `--keys-only`) to export a snapshot with size columns.

### find

```bash
# Search keys containing "index"
etcdctl+ find --match-key=index

# By prefix
etcdctl+ find --prefix=/registry/pods

# Search and show value
etcdctl+ find --match-key=index --value

# Limit returned count (pushed down to etcd server; safe even for large prefixes)
etcdctl+ find --match-key=index --limit=50
```

### wal-look

Parse WAL from an etcd data directory and export the operation stream. **Read-only, no flock — safe to run directly on production `member/wal`** (same mechanism as the official `etcd-dump-logs`; does not block the cluster or corrupt data).

```bash
# Export WAL operations as JSONL (recommended: structured, consumable by wal-summary --input)
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=jsonl --output=wal.jsonl

# --data-dir accepts both: an etcd data dir (with member/wal) or a WAL dir (with *.wal) directly
etcdctl+ wal-look --data-dir=/var/lib/etcd/member/wal --write-out=jsonl --output=wal.jsonl

# Operations in an index range only
etcdctl+ wal-look --data-dir=/var/lib/etcd --start-index=930 --end-index=1000 \
    --write-out=jsonl --output=wal-range.jsonl

# Filter by entry type (--entry-type supports all 17 types; see table below; -h lists all)
etcdctl+ wal-look --data-dir=/var/lib/etcd --entry-type=IRRPut,IRRDeleteRange \
    --write-out=jsonl --output=wal-writes.jsonl

# Terminal view (default stdout)
etcdctl+ wal-look --data-dir=/var/lib/etcd

# Other output formats (not preferred for offline analysis):
#   --write-out=log  log format, for ingestion by loki etc.; unstructured, not consumable by wal-summary --input
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=log --output=wal.log
```

JSONL output (one WalOp per line; ops without key/value such as Compaction omit those fields):

```json
{"raft_index":10864,"raft_term":3,"op_type":"Put","key":".monitor","value_size_bytes":17,"entry_type":"IRRPut"}
{"raft_index":10865,"raft_term":3,"op_type":"Compaction","entry_type":"IRRCompaction"}
{"raft_index":10866,"raft_term":3,"op_type":"DeleteRange","key":"/old","entry_type":"IRRDeleteRange"}
```

**`op_type` ↔ `entry_type` mapping** (also listed by `-h`):

| op_type | entry_type | notes |
|---|---|---|
| Range | IRRRange | read; not normally in WAL — its presence is anomalous |
| Put | IRRPut | write / overwrite |
| DeleteRange | IRRDeleteRange | range delete |
| Txn | IRRTxn | txn parent record; sub-ops keep entry_type IRRTxn, is_txn=true |
| Compaction | IRRCompaction | compaction; tiny payload in WAL |
| LeaseGrant | IRRLeaseGrant | grant lease |
| LeaseRevoke | IRRLeaseRevoke | revoke lease |
| LeaseCheckpoint | IRRLeaseCheckpoint | lease checkpoint (KeepAlive is not written to WAL) |
| AuthEnable | IRRAuthEnable | enable auth |
| AuthDisable | IRRAuthDisable | disable auth |
| AuthUser | IRRAuthUser | user add/delete / password / grant |
| AuthRole | IRRAuthRole | role add/delete / grant |

Plus 5 fallback types: `IRRUnknown`, `ConfigChange`, `Normal`, `Request`, `Unknown`.

> 💡 **WAL write-pressure attribution:** Put + DeleteRange (including Txn sub-ops) dominate WAL volume; other types together are typically <5%. Txn sub-ops have `op_type` Put/DeleteRange and are already counted in wal-summary's PutCount/DeleteCount — no undercounting.

### wal-summary

Aggregate WAL write counts per key, from a WalOp JSONL or by parsing WAL directly. **Answers "which key is written most".**

```bash
# Offline (recommended): analyze JSONL exported by wal-look repeatedly without re-parsing WAL
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --top=50

# Parse WAL directly (one step, but re-parses each time; fine for small clusters or one-off checks)
etcdctl+ wal-summary --data-dir=/var/lib/etcd --sort=put-count --top=50

# Delete hotspots
etcdctl+ wal-summary --input=wal.jsonl --sort=delete-count --top=50

# By total ops
etcdctl+ wal-summary --input=wal.jsonl --sort=total-ops --top=50

# JSON output for programs (wal-summary supports text/json only)
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --write-out=json --output=wal-summary.json
```

Text output prints an **entry-type distribution table** above the Top N by default (covers all 17 types; answers "operation-type distribution"):

```text
Entry-Type Distribution (17000 total ops):
  IRRPut             12000
  IRRDeleteRange       800
  IRRTxn               150  (parent; sub-ops already counted in IRRPut/IRRDeleteRange)
  IRRCompaction         12
  IRRLeaseGrant        ...

WAL Summary: 17000 total ops, 3500 unique keys (top 50 by put-count)

Key                                                            PutCount  DeleteCount     TotalValueSize
.monitor                                                          12000            0            204.0KiB
/registry/pods/default/nginx-deploy-abc                           2500           50             40.0MiB
...
```

`--sort`: `put-count` (default) / `delete-count` / `total-ops`.

### dump

Export raw entries from a snapshot db or WAL (plain text, aligned with `etcd-dump-db`/`etcd-dump-logs`; does not use the JSONL pipeline).

```bash
# db: list buckets
etcdctl+ dump list-bucket --snapshot=snapshot.db

# db: iterate bucket entries (--decode as mvccpb.KeyValue, --limit caps entries)
etcdctl+ dump iterate-bucket --snapshot=snapshot.db key --decode --limit=100

# db: scan by revision range
etcdctl+ dump scan-keys --snapshot=snapshot.db --start-revision=100 --end-revision=200 --limit=50

# WAL: raw entries (--entry-type same as wal-look, all 17 types; -h lists all)
etcdctl+ dump wal --data-dir=/var/lib/etcd --entry-type=IRRPut --start-index=930 --end-index=932
```

### unmarshal

```bash
# Decode protobuf data in etcd
etcdctl+ unmarshal \
  --target-key /registry/pods/default/my-pod \
  --import-path /path/to/proto/dir \
  --proto /path/to/proto/dir/api.proto \
  --full-message-name k8s.io.api.core.v1.Pod
```

### leader

```bash
etcdctl+ leader
```

### decode

```bash
# Decode a base64 value (local only, no etcd connection)
etcdctl+ decode --value="aGVsbG8gd29ybGQ="
```

### TLS example

```bash
# All commands accept the global TLS flags
etcdctl+ \
  --endpoints=https://etcd.example.com:2379 \
  --cert=/path/to/client.pem \
  --key=/path/to/client-key.pem \
  --cacert=/path/to/ca.pem \
  distribute --type=value
```
