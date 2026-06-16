# Changelog

---

## Phase 1: Initial Fixes

> Commit `d89a6ea` — Fix `WithPrefix` panic, `--key` flag collision, disable high-risk commands.

### Fix 1: etcd client v3.5.0 `WithPrefix` false-positive panic

**Symptoms**

Running `distribute`, `look`, or `find` (without prefix) triggers a panic:

```
panic: `WithPrefix` and `WithFromKey` cannot be set at the same time, choose one
```

**Root Cause**

etcd client v3.5.0 uses reflection + `strings.Contains` to detect option types. The function name `GetDataWithPrefix` contains `WithPrefix`, causing all closure variable names within (such as `WithFromKey`, `WithSerializable`, `WithLimit`) to be falsely matched as `WithPrefix`, triggering the conflict detection.

**Fix**

Upgrade etcd client from v3.5.0 to v3.5.27. Since v3.5.15+, options set boolean flags directly inside closures instead of relying on reflection and string matching.

**Files modified:** `go.mod`, `go.sum`

```
go.etcd.io/etcd/api/v3        v3.5.0  → v3.5.27
go.etcd.io/etcd/client/pkg/v3 v3.5.0  → v3.5.27
go.etcd.io/etcd/client/v3     v3.5.0  → v3.5.27
go directive                   1.18   → 1.24
```

---

### Fix 2: `--key` Flag collision causing TLS handshake failure

**Symptoms**

When connecting to etcd with TLS, `find --key=xxx` and `unmarshal --key=xxx` fail with:

```
tls: failed to verify certificate: x509: "etcd" certificate is not standards compliant
```

But `leader`, `distribute`, `look` etc. work fine with TLS.

**Root Cause**

The global PersistentFlag `--key` (TLS private key file) clashes with the subcommand LocalFlag `--key` (search keyword/etcd key). Cobra's subcommand LocalFlag overrides the PersistentFlag, causing the TLS key path to be overwritten with the search keyword string.

```go
// Global flag
rootCmd.PersistentFlags().StringVar(&core.C.TLS.KeyFile, "key", "", "TLS key file")

// Subcommand flag (name collision!)
cmd.Flags().StringVar(&findKey, "key", "", "search keyword")
cmd.Flags().StringVar(&unmarshallKey, "key", "", "etcd key")
```

**Trigger Conditions**

Both conditions must be true: 1) TLS connection in use; 2) subcommand uses `--key`

| Scenario | Result |
|----------|--------|
| No TLS + `find --key=qa` | OK (global --key is empty, override has no effect) |
| TLS + `find` (no --key) | OK (global --key not overridden) |
| TLS + `find --key=qa` | TLS key overwritten to "qa", handshake fails |

**Fix**

Rename the subcommand `--key` flags to avoid collision with the global TLS `--key`.

| Command | Before | After |
|---------|--------|-------|
| find | `--key` | `--match-key` |
| unmarshal | `--key` | `--target-key` |

Naming follows the existing pattern `--source-key`, `--target-key`, `--filter-max`.

**Files modified:** `cmd/find_cmd.go`, `cmd/unmarsha_cmd.go`

---

### Fix 3: Disable high-risk commands

**Reason**

`clear` and `rename` commands pose data security risks:

| Command | Risk Level | Description |
|---------|-----------|-------------|
| clear | Critical | Deletes ALL etcd data, irreversible |
| rename | High | Non-atomic Get→Put→Delete, may cause inconsistency on failure |

**Fix**

Comment out `NewClearCmd()` and `NewRenameCmd()` registration in `root_cmd.go`. Source code is preserved; uncomment to re-enable.

**Files modified:** `cmd/root_cmd.go`

---

## Phase 2: JSON Output Feature

> Commit `1f1cc55` (feat) — distribute command adds `--write-out=json` support, outputting machine-readable JSON reports for scripting and automation.
>
> Commits `fce35f0`~`0481267` (fix/test) — Based on Code Review feedback, the following fixes were applied and a complete test suite was established.

---

### CR-Fix 1: `histogramJSON()` incorrect bucket results (Critical)

**Problem**

`JSON()` called `histogramJSON()` before `percentilesJSON()`. The two-pointer bucket algorithm in `histogramJSON()` requires `r.stats.sizes` to be sorted (pointer `bi` only moves forward), but `sizes` was not yet sorted at that point. Text mode `String()` correctly calls `sort.Ints` before `histogram()`, but the JSON mode had the order wrong.

**Fix**

Add `sort.Ints(r.stats.sizes)` at the beginning of `JSON()`, and remove the redundant sort inside `percentilesJSON()`.

**Files modified:** `core/report.go`

---

### CR-Fix 2: Empty data exposes sentinel values in JSON mode

**Problem**

When `Count == 0`, `Smallest` retains the initial sentinel value `math.MaxInt32` and `Largest` stays `-1`. Text mode has an empty data guard (outputs `"empty data"` and skips `finalString()`), but JSON mode would output meaningless sentinel values.

**Fix**

Add an empty data check at the start of `JSON()`, returning a reasonable zero-value structure:

```go
if r.stats.Count <= 0 {
    empty := ReportJSON{
        Summary:     SummaryJSON{},
        Histogram:   []BucketJSON{},
        Percentiles: map[string]int{},
    }
    data, _ := json.MarshalIndent(empty, "", "  ")
    return string(data)
}
```

**Files modified:** `core/report.go`

---

### CR-Fix 3: `NewReport` variadic bool → Functional Options

**Problem**

`NewReport(bc, of, true)` — the meaning of `true` is opaque to the caller; unclear API design.

**Fix**

Introduce the Functional Options pattern:

```go
type ReportOption func(*report)

func WithJSONMode() ReportOption { ... }

func NewReport(bc int, of SizeOf, opts ...ReportOption) Report { ... }
```

Caller code becomes:

```go
// JSON mode
r := core.NewReport(bucketCount, sizeOf, core.WithJSONMode())

// Text mode (default)
r := core.NewReport(bucketCount, sizeOf)
```

**Files modified:** `core/report.go`, `cmd/distribute_cmd.go`

---

### CR-Fix 4: Pointless `processResults` sleep in JSON mode

**Problem**

`processResults()` had a fixed `time.Sleep(100ms)` at the end to allow text mode's dynamic output to flush. JSON mode has no dynamic output, making this sleep a waste of time.

**Fix**

Make it conditional:

```go
if !r.jsonMode {
    time.Sleep(time.Millisecond * 100)
}
```

**Files modified:** `core/report.go`

---

### CR-Fix 5: Text/JSON code duplication in `distribute_cmd.go`

**Problem**

The text and JSON branches duplicated the entire data pipeline logic, differing only in `DynamicOutput()` and the final output format.

**Fix**

Extract the common data pipeline, branching only for Report construction and final output:

```go
isJSON := distributeWriteOut == "json"
var r core.Report
if isJSON {
    r = core.NewReport(bucketCount, sizeOf, core.WithJSONMode())
} else {
    r = core.NewReport(bucketCount, sizeOf)
}
// Common data pipeline
c1 := r.Results()
go func() {
    defer close(c1)
    if !isJSON && len(datac) > 0 {
        r.DynamicOutput()
    }
    for data := range datac { c1 <- data }
}()
<-r.Run()
if isJSON {
    fmt.Println(r.JSON())
}
```

**Files modified:** `cmd/distribute_cmd.go`

---

### CR-Fix 6: Stale old-version hashes in `go.sum`

**Problem**

After upgrading etcd client, `go.sum` retained both v3.5.0 and v3.5.27 hashes.

**Fix**

Run `go mod tidy`, removing 219 lines of stale hashes.

**Files modified:** `go.sum`

---

### CR-Fix 7: Ambiguous unit in `ReportJSON.Percentiles` values

**Problem**

Percentile values were output as bare numbers `"p50": 424` with no indication of unit. Text mode shows `424.0 Byte` which is readable, but JSON mode lacked unit context.

**Fix**

Change the JSON tag from `percentiles` to `percentiles_bytes` and key names from `p50` to `p50_bytes`, making the byte unit explicit:

```json
// Before
"percentiles": { "p50": 424 }

// After
"percentiles_bytes": { "p50_bytes": 424 }
```

**Files modified:** `core/report.go`

---

### CR-Fix 8: `countLock` protection inconsistency

**Problem**

`String()` used `countLock.RLock()/RUnlock()` when calling `PrintPercent()`, but `percentilesJSON()` called `percentiles()` without any lock. While `JSON()` is called after `Run()` completes (safe in practice), the inconsistency with `String()`'s locking pattern creates a maintenance trap.

**Fix**

Add `countLock.RLock()/RUnlock()` in `percentilesJSON()`, consistent with `String()`.

**Files modified:** `core/report.go`

---

### CR-Fix 9: Data race in `String()` `sort.Ints`

**Problem**

`DynamicOutput()` calls `dynamicString()` → `String()` → `sort.Ints(r.stats.sizes)` every 100ms, while `processResult()` concurrently appends to `sizes`. `sort.Ints` mutates the slice in place without holding the lock, causing a data race with `append`. When `append` triggers slice growth, `sort` may read an inconsistent slice header, potentially causing a panic.

**Fix**

Wrap `sort.Ints` + `histogram()` + `PrintPercent()` in `String()` with a single `countLock.Lock()`/`Unlock()`, and remove the redundant per-element locks inside `histogram()`:

```go
r.stats.countLock.Lock()
sort.Ints(r.stats.sizes)
buffer.WriteString(r.histogram())
buffer.WriteString(PrintPercent(...))
r.stats.countLock.Unlock()
```

**Files modified:** `core/report.go`

---

### CR-Fix 10: Field-level data race in `processResult`

**Problem**

`go test -race` detected that `processResult()` writes to `r.stats.Count`/`Smallest`/`Largest`/`Total`/`Average` without holding a lock, while `dynamicString()` and `finalString()` read these fields concurrently.

**Fix**

1. `processResult()`: Wrap all stat field writes in `countLock.Lock()/Unlock()`
2. `dynamicString()` / `finalString()`: Protect `Count` reads with `countLock.RLock()`

**Files modified:** `core/report.go`

---

## Fix Status Summary

| # | Issue | Severity | Status |
|---|-------|----------|--------|
| 1 | `WithPrefix` reflection false-positive panic | 🔴 Critical | ✅ Fix 1 |
| 2 | `--key` flag collision causing TLS failure | 🔴 Critical | ✅ Fix 2 |
| 3 | clear/rename high-risk commands | 🔴 Critical | ✅ Fix 3 |
| 4 | `histogramJSON()` unsorted bucket error | 🔴 Critical | ✅ CR-Fix 1 |
| 5 | JSON empty data exposes sentinel values | 🟡 Medium | ✅ CR-Fix 2 |
| 6 | `NewReport` variadic bool API unclear | 🟡 Medium | ✅ CR-Fix 3 |
| 7 | Pointless sleep in JSON mode | 🟡 Medium | ✅ CR-Fix 4 |
| 8 | Text/JSON duplicated code | 🟡 Medium | ✅ CR-Fix 5 |
| 9 | Stale hashes in `go.sum` | 🟢 Low | ✅ CR-Fix 6 |
| 10 | Ambiguous percentile units | 🟢 Low | ✅ CR-Fix 7 |
| 11 | `countLock` protection inconsistency | 🟡 Medium | ✅ CR-Fix 8 |
| 12 | `String()` `sort.Ints` data race | 🔴 Critical | ✅ CR-Fix 9 |
| 13 | `processResult` field-level data race | 🔴 Critical | ✅ CR-Fix 10 |
| 14 | `go 1.18 -> 1.24` large version jump | 🟡 Medium | ⏭️ Skipped (needs CI env confirmation) |
| 15 | Commented-out commands are hardcoded | 🟡 Medium | ⏭️ Skipped (process suggestion, not a code bug) |

---

## Files Modified Summary

| File | Changes |
|------|--------|
| `go.mod` | etcd client v3.5.0 -> v3.5.27, go 1.18 -> 1.24 |
| `go.sum` | Dependency hash updates; `go mod tidy` cleanup of 219 stale lines |
| `cmd/root_cmd.go` | Disabled clear and rename commands |
| `cmd/find_cmd.go` | `--key` -> `--match-key` |
| `cmd/unmarsha_cmd.go` | `--key` -> `--target-key` |
| `cmd/distribute_cmd.go` | Added JSON output; uses `WithJSONMode()`; extracted common text/json logic |
| `core/report.go` | Histogram sort fix; empty data guard; Functional Options; conditional sleep; explicit units; countLock consistency; data race fixes |

---

> See [tests/README.md](tests/README.md) for the test suite documentation.
