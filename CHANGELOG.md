# Changelog

This document follows the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

---

## [Unreleased] — Phase 4: Offline Analysis

> Design doc: `docs/etcd-offline-design/etcd-offline-analysis-design.md`

### Added

- **`look --snapshot` offline data source**: Parses bbolt snapshot db file, outputs all fields in a single pass (current-state KV + value size + rev_count + tombstone_count), no cluster connection required
- **`wal-look` command**: Parses WAL logs, outputs WalOp entries one by one (raft_index/op_type/key/value_size_bytes); supports `--entry-type` filtering, `--start/end-index` range
- **`wal-summary` command**: Reads WalOp JSONL or parses WAL directly → aggregates Put/Delete counts per key → `--sort=put-count/delete-count` sorted Top N
- **`dump` command set**: Subcommands `list-bucket` / `iterate-bucket` (+ size enhancement) / `scan-keys` / `wal`, raw plaintext export
- **`summary --sort=rev-count/tombstone-count`**: New sort dimensions for offline snapshot JSONL, locating historical revision accumulation and frequent-deletion hotspots
- **`distribute --input` / `find --input`**: Consume KeyMeta JSONL for offline analysis, aligned with `summary --input`
- **`core/snapshot_source.go`**: `SnapshotSource` single-pass all fields, adapted from `ahrtr/etcd-diagnosis`'s `BytesToBucketKey` (~60 lines)
- **`core/wal_source.go`**: `WalSource` + `WalOp` struct + all 12 entry-types
- **`core/revision.go`**: `BytesToBucketKey` + tombstone detection

### Changed

- **`KeyMeta`** adds `RevCount` / `TombstoneCount` fields, JSONL read/write adapted
- **`look` help text**: `--keys-only` annotated "online only; ignored with --snapshot"; `--snapshot` notes "outputs all fields in a single pass"

### Design Decisions

- **Single-pass design**: Offline `--snapshot` does not split into `--keys-only` and `--rev-count` modes; one bbolt traversal performs current-state dedup + rev_count/tombstone_count aggregation simultaneously, time complexity unchanged (O(N))
- **JSONL pipeline**: `--snapshot` only on the `look` command; other commands consume JSONL via `--input` (load once, analyze many times)
- **Reference implementation**: Core references `ahrtr/etcd-diagnosis` offline (rev_count analysis), copied rather than imported (v3.6.7 vs v3.5.27 version difference)
- **Dependency changes**: + `bbolt` (snapshot db) + `server/v3` (only WAL path requires `wal.OpenForRead`)

---

## [0.3.0] — 2026-06-23 — Phase 3: Online Refactoring

> Commit `34ff4e2` — Add summary command + shared filter/meta modules + JSONL pipeline + low-risk inspection workflow.
>
> Refactoring plan: see `docs/changelog/etcd-analysis-key-improvement-plan.md`.

### Added

- **`summary` command**: Aggregate Top N by prefix, supports `--group-depth` / `--sort` / `--top` / `--input` (offline JSONL) / `--keys-only` (online) / `--min/max-create/mod-revision`
- **`look --keys-only`**: Online mode uses `WithKeysOnly()` under the hood, does not fetch values, low-risk snapshot
- **`look --write-out=jsonl`**: Stream export KeyMeta JSONL for `summary --input` offline multi-pass analysis
- **`look --prefix` / `distribute --prefix`**: Server-side prefix scoping, no full scan
- **`look --page-size` / `--page-sleep`**: Scan rate control, reduces follower burst pressure
- **`core/meta.go`**: `KeyMeta` record + JSONL read/write
- **`core/filter.go`**: Shared size filter, reused by look/summary
- **`core/summary.go`**: Prefix aggregation engine `GroupStats` / `Summarize` / `GroupPrefix`

### Changed

- **`look` size field split**: Log/JSONL output changed from a single `size` field whose meaning varied with `--filter`, to fixed-semantics `key_size_bytes` / `value_size_bytes` / `kv_size_bytes` / `kv_size_human`
- **`find --limit` pushed to server**: `WithLimit` pushed directly to etcd Range, safe even for large prefixes
- **`data_source.go`**: Supports `WithKeysOnly()` / `WithPrefix()` / page-size / page-sleep

### Files Modified

| File | Changes |
|------|---------|
| `cmd/summary_cmd.go` | New summary command |
| `cmd/look_cmd.go` | keys-only / jsonl / prefix / page-size / page-sleep / filter refactoring |
| `cmd/distribute_cmd.go` | --prefix support |
| `cmd/find_cmd.go` | --limit pushed to server |
| `cmd/root_cmd.go` | Register summary |
| `core/meta.go` | KeyMeta + JSONL read/write |
| `core/filter.go` | Shared FilterConfig |
| `core/summary.go` | Prefix aggregation engine |
| `core/data_source.go` | keys-only / prefix / page control |
| `core/report.go` | Adapted to new helpers |

---

## [0.2.0] — 2026-06-18 — Phase 2: JSON Output Feature

> Commit `336b210` (feat) — distribute command adds `--write-out=json` support, outputting machine-readable JSON reports for scripting and automation.
>
> Commits `e0dde72`~`a18fbf6` (fix/test) — Based on Code Review feedback from the above two commits, fixes were applied incorporating multiple review opinions, and a complete test suite was established.

### Added

- **`distribute --write-out=json`**: Machine-readable JSON output (summary/histogram/percentiles)

### Fixed

- **CR-Fix 1**: `histogramJSON()` incorrect bucket results — unsorted `sizes` caused two-pointer algorithm failure (Critical)
- **CR-Fix 2**: JSON mode empty data exposes `math.MaxInt32` / `-1` sentinel values
- **CR-Fix 3**: `NewReport(bc, of, true)` variadic bool → Functional Options (`WithJSONMode()`)
- **CR-Fix 4**: Pointless `100ms` sleep in `processResults` under JSON mode
- **CR-Fix 5**: Duplicated data pipeline code across text/json branches → extracted common logic
- **CR-Fix 6**: Stale v3.5.0 hashes in `go.sum` → `go mod tidy` cleaned 219 lines
- **CR-Fix 7**: `ReportJSON.Percentiles` values had ambiguous units → added `_bytes` suffix to key names
- **CR-Fix 8**: `percentilesJSON()` missing `countLock.RLock()` protection (inconsistent with `String()`)
- **CR-Fix 9**: `sort.Ints` in `String()` races with `processResult`'s `append` — data race (Critical)
- **CR-Fix 10**: `processResult` writes `Count`/`Smallest`/`Largest`/`Total`/`Average` without holding lock (Critical)

### Files Modified

| File | Changes |
|------|---------|
| `cmd/distribute_cmd.go` | Added JSON output; uses `WithJSONMode()`; extracted text/json common logic |
| `core/report.go` | Histogram bucket sort; empty data guard; Functional Options; conditional sleep; explicit units; countLock consistency; data race fixes |
| `go.sum` | `go mod tidy` cleanup |

> See [tests/README.md](tests/README.md) for the test suite documentation.

---

## [0.1.0] — 2026-06-16 — Phase 1: Initial Fixes

> Commit `8751b88` — Fix `WithPrefix` panic, flag collision, disable high-risk commands.

### Fixed

- **Fix 1**: etcd client v3.5.0 `WithPrefix` reflection false-positive panic — upgraded etcd client v3.5.0 → v3.5.27
- **Fix 2**: `--key` flag collision causing TLS connection failure — `find --key` → `--match-key`, `unmarshal --key` → `--target-key`
- **Fix 3**: Disabled `clear` (Critical: deletes all data, irreversible) and `rename` (High: non-atomic Get→Put→Delete) high-risk commands

### Files Modified

| File | Changes |
|------|---------|
| `go.mod` | etcd client v3.5.0 → v3.5.27, go 1.18 → 1.24 |
| `go.sum` | Dependency hash updates |
| `cmd/root_cmd.go` | Disabled clear and rename commands |
| `cmd/find_cmd.go` | `--key` → `--match-key` |
| `cmd/unmarsha_cmd.go` | `--key` → `--target-key` |

---

## Fix Status Summary

| # | Issue | Severity | Phase | Status |
|---|-------|----------|-------|--------|
| 1 | `WithPrefix` reflection false-positive panic | Critical | 1 | ✅ Fix 1 |
| 2 | `--key` flag collision causing TLS failure | Critical | 1 | ✅ Fix 2 |
| 3 | clear/rename high-risk commands | Critical | 1 | ✅ Fix 3 |
| 4 | `histogramJSON()` unsorted bucket error | Critical | 2 | ✅ CR-Fix 1 |
| 5 | JSON empty data exposes sentinel values | Medium | 2 | ✅ CR-Fix 2 |
| 6 | `NewReport` variadic bool API unclear | Medium | 2 | ✅ CR-Fix 3 |
| 7 | Pointless sleep in JSON mode | Medium | 2 | ✅ CR-Fix 4 |
| 8 | Text/JSON duplicated code | Medium | 2 | ✅ CR-Fix 5 |
| 9 | Stale hashes in `go.sum` | Low | 2 | ✅ CR-Fix 6 |
| 10 | Ambiguous percentile units | Low | 2 | ✅ CR-Fix 7 |
| 11 | `countLock` protection inconsistency | Medium | 2 | ✅ CR-Fix 8 |
| 12 | `String()` `sort.Ints` data race | Critical | 2 | ✅ CR-Fix 9 |
| 13 | `processResult` field-level data race | Critical | 2 | ✅ CR-Fix 10 |
| 14 | `go 1.18 -> 1.24` large version jump | Medium | — | ⏭️ Skipped (needs CI env confirmation) |
| 15 | Commented-out commands are hardcoded | Medium | — | ⏭️ Skipped (process suggestion, not a code bug) |
