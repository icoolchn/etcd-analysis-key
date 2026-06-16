# etcd-analysis Test Suite

This directory is an independent Go module that references the main module via a `replace` directive. It follows a layered test architecture aligned with etcd's `tests/` structure.

## Directory Layout

```
tests/
├── go.mod                          # Independent module, replace => ../
├── go.sum
├── common/
│   └── helpers.go                  # Shared utilities: MakeKVs, FeedAndRun, ParseReportJSON
├── unit/
│   └── report_api_test.go          # Exported API unit tests
├── integration/
│   └── distribute_test.go          # Integration + JSON contract tests
├── benchmark/
│   └── report_bench_test.go        # Performance benchmarks
├── race/
│   └── race_test.go                # Concurrency race tests
├── regression/
│   └── bugfix_test.go              # Bugfix regression tests (CR-Fix 1~10)
└── stress/
    └── stress_test.go              # Stress tests (large data, high concurrency)

core/
└── report_test.go                  # Internal unit tests (access to unexported types)
```

## Test Coverage Summary

| Test File | Count | Type | Description |
|-----------|-------|------|-------------|
| `core/report_test.go` | 10 | Internal unit | Field assignment, sort ordering, lock behavior, empty data, JSON output |
| `tests/unit/` | 3 | Exported API | NewReport construction, WithJSONMode option |
| `tests/integration/` | 5 | Integration + Contract | Full data pipeline, JSON schema validation |
| `tests/benchmark/` | 4 | Benchmark | Throughput, JSON output latency, large key sets |
| `tests/race/` | 3 | Race | Multi-goroutine concurrent writes, DynamicOutput |
| `tests/regression/` | 7 | Regression | CR-Fix 1~10 regression protection |
| `tests/stress/` | 3 | Stress | 10K keys, 100-batch concurrent writes, large values |
| **Total** | **35** | | |

## Running Tests

All test commands are unified under the root `Makefile`:

```bash
# Run all tests (internal unit + tests/ module)
make test

# Run individual layers
make test-unit              # Internal unit tests (core/report_test.go)
make test-external          # All tests in tests/ module
make test-integration       # Integration tests only
make test-race              # Race detection (auto-append -race flag)
make test-regression        # Regression tests (auto-append -race flag)
make test-stress            # Stress tests (timeout 60s)
make test-bench             # Benchmarks (output benchmem)

# Utilities
make build                  # Build binary
make vet                    # go vet static analysis
make tidy                   # go mod tidy (main module + tests/ module)

# Convenience: run specific tests
make test-run RUN="TestNewReport"               # Run matching tests
make test-run RUN="TestNewReport" DIR="./core"  # Run in specific directory
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GO_TEST_FLAGS` | (empty) | Extra arguments passed to `go test` |
| `RUN` | (empty) | `-run` regex filter, used with `test-run` |
| `DIR` | `./...` | Test directory, used with `test-run` |

### Examples

```bash
# Run regression tests with verbose output and count
GO_TEST_FLAGS="-v -count=5" make test-regression

# Run a specific test function
make test-run RUN="TestCRFix3_FunctionalOptions" DIR="./tests/regression"

# Race detection with verbose output
GO_TEST_FLAGS="-v" make test-race
```

## Writing Tests Guide

### Choosing a Test Layer

| Scenario | Recommended Layer |
|----------|-----------------|
| Verify unexported fields or internal behavior | `core/report_test.go` (internal unit) |
| Verify exported public API | `tests/unit/` |
| Full multi-module end-to-end flow | `tests/integration/` |
| Concurrency safety | `tests/race/` |
| Regression protection after bugfix | `tests/regression/` |
| Performance benchmarks | `tests/benchmark/` |
| Stability under extreme conditions | `tests/stress/` |

### Using Shared Utilities

`tests/common/helpers.go` provides:

- `MakeKVs(sizes []int) []*mvccpb.KeyValue` — Generate mock KV data with specified sizes
- `SizeOfKey(kv *mvccpb.KeyValue) int` — Calculate total bytes of key + value
- `FeedAndRun(t, r, sizes)` — Feed data and trigger report generation
- `ParseReportJSON(t, output)` — Parse JSON output into `ReportJSON` struct
- `FeedAndRunB(b, r, sizes)` — Benchmark version of FeedAndRun

### Naming Conventions

- Internal tests: `Test<FunctionName>_<Scenario>`
- Regression tests: `TestCRFix<N>_<Description>`, corresponding to CR-Fix numbers in [CHANGELOG.md](../CHANGELOG.md)
- Benchmarks: `Benchmark<FunctionName>`

## Bugfix Regression Protection

`tests/regression/bugfix_test.go` provides regression tests for each fix documented in [CHANGELOG.md](../CHANGELOG.md):

| Test Name | Corresponding Fix | Verification |
|-----------|------------------|--------------|
| `TestCRFix1_HistogramSort` | CR-Fix 1 | Histogram buckets sorted by count descending |
| `TestCRFix2_EmptyDataGuard` | CR-Fix 2 | Empty data JSON output does not panic |
| `TestCRFix3_FunctionalOptions` | CR-Fix 3 | WithJSONMode correctly sets jsonMode |
| `TestCRFix4_NoSleepInJSON` | CR-Fix 4 | time.Sleep not executed in JSON mode |
| `TestCRFix7_BytesSuffix` | CR-Fix 7 | Percentile keys contain `_bytes` suffix |
| `TestCRFix8_CountLockConsistency` | CR-Fix 8 | percentilesJSON read lock consistency |
| `TestCRFix10_ProcessResultRace` | CR-Fix 10 | processResult field-level race protection |
