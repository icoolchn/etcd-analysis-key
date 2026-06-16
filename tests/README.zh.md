# etcd-analysis-key 测试体系

本目录为独立 Go 模块，通过 `replace` 指令引用主模块，采用与 etcd `tests/` 对齐的分层测试架构。

## 目录结构

```
tests/
├── go.mod                          # 独立模块，replace => ../
├── go.sum
├── common/
│   └── helpers.go                  # 共享工具：MakeKVs, FeedAndRun, ParseReportJSON
├── unit/
│   └── report_api_test.go          # 导出 API 单元测试
├── integration/
│   └── distribute_test.go          # 集成测试 + JSON 契约测试
├── benchmark/
│   └── report_bench_test.go        # 基准性能测试
├── race/
│   └── race_test.go                # 并发竞态测试
├── regression/
│   └── bugfix_test.go              # BUGFIX 回归测试（CR-Fix 1~10）
└── stress/
    └── stress_test.go              # 压力测试（大数据量、高并发）

core/
└── report_test.go                  # 内部单元测试（可访问未导出类型）
```

## 测试覆盖汇总

| 测试文件 | 测试数 | 类型 | 说明 |
|---------|-------|------|------|
| `core/report_test.go` | 10 | 内部单元 | 字段赋值、sort 排序、锁行为、空数据、JSON 输出 |
| `tests/unit/` | 3 | 导出 API | NewReport 构造、WithJSONMode 选项 |
| `tests/integration/` | 5 | 集成+契约 | 完整数据管道、JSON schema 结构验证 |
| `tests/benchmark/` | 4 | 基准 | 吞吐量、JSON 输出耗时、大 key 集 |
| `tests/race/` | 3 | 竞态 | 多 goroutine 并发写、DynamicOutput |
| `tests/regression/` | 7 | 回归 | CR-Fix 1~10 各项修复的回归保护 |
| `tests/stress/` | 3 | 压力 | 10K key、100 批并发写入、超大 value |
| **合计** | **35** | | |

## 运行方式

所有测试命令均通过根目录 `Makefile` 统一入口：

```bash
# 运行全部测试（内部单元 + tests/ 模块）
make test

# 各分层单独运行
make test-unit              # 内部单元测试（core/report_test.go）
make test-external          # tests/ 模块全部测试
make test-integration       # 仅集成测试
make test-race              # 竞态检测（自动附加 -race 标志）
make test-regression        # 回归测试（自动附加 -race 标志）
make test-stress            # 压力测试（timeout 60s）
make test-bench             # 基准测试（输出 benchmem）

# 工具类
make build                  # 构建二进制
make vet                    # go vet 静态检查
make tidy                   # go mod tidy（主模块 + tests/ 模块）

# 便捷：运行指定测试
make test-run RUN="TestNewReport"               # 运行匹配的测试
make test-run RUN="TestNewReport" DIR="./core"  # 在指定目录运行
```

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `GO_TEST_FLAGS` | （空） | 传递额外参数给 `go test` |
| `RUN` | （空） | `-run` 正则过滤，配合 `test-run` 使用 |
| `DIR` | `./...` | 指定测试目录，配合 `test-run` 使用 |

### 示例

```bash
# 带 verbose 和 count 运行回归测试
GO_TEST_FLAGS="-v -count=5" make test-regression

# 只运行某个测试函数
make test-run RUN="TestCRFix3_FunctionalOptions" DIR="./tests/regression"

# 竞态检测 + 详细输出
GO_TEST_FLAGS="-v" make test-race
```

## 编写测试指南

### 选择测试层级

| 场景 | 推荐层级 |
|------|---------|
| 验证未导出字段或内部行为 | `core/report_test.go`（内部单元） |
| 验证导出的公共 API | `tests/unit/` |
| 多模块协作的完整流程 | `tests/integration/` |
| 并发安全性 | `tests/race/` |
| 修复某个 bug 后的回归保护 | `tests/regression/` |
| 性能基准 | `tests/benchmark/` |
| 极端条件下的稳定性 | `tests/stress/` |

### 使用共享工具

`tests/common/helpers.go` 提供以下工具函数：

- `MakeKVs(sizes []int) []*mvccpb.KeyValue` — 按指定大小生成 mock KV 数据
- `SizeOfKey(kv *mvccpb.KeyValue) int` — 计算 key+value 总字节数
- `FeedAndRun(t, r, sizes)` — 填充数据并触发报告生成
- `ParseReportJSON(t, output)` — 解析 JSON 输出为 `ReportJSON` 结构体
- `FeedAndRunB(b, r, sizes)` — benchmark 版本的 FeedAndRun

### 命名约定

- 内部测试：`Test<FunctionName>_<Scenario>`
- 回归测试：`TestCRFix<N>_<Description>`，与 `CHANGELOG.zh.md` 中的 CR-Fix 编号对应
- 基准测试：`Benchmark<FunctionName>`

## Bugfix 回归保护

`tests/regression/bugfix_test.go` 为 [CHANGELOG.zh.md](../CHANGELOG.zh.md) 中记录的每项修复提供回归测试：

| 测试名 | 对应修复 | 验证内容 |
|--------|---------|---------|
| `TestCRFix1_HistogramSort` | CR-Fix 1 | histogram 分桶按 count 降序排列 |
| `TestCRFix2_EmptyDataGuard` | CR-Fix 2 | 空数据 JSON 输出不 panic |
| `TestCRFix3_FunctionalOptions` | CR-Fix 3 | WithJSONMode 正确设置 jsonMode |
| `TestCRFix4_NoSleepInJSON` | CR-Fix 4 | JSON 模式下不执行 time.Sleep |
| `TestCRFix7_BytesSuffix` | CR-Fix 7 | 百分位 key 含 `_bytes` 后缀 |
| `TestCRFix8_CountLockConsistency` | CR-Fix 8 | percentilesJSON 读锁一致性 |
| `TestCRFix10_ProcessResultRace` | CR-Fix 10 | processResult 字段级竞态保护 |
