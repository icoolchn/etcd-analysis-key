# Changelog

本文档遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式。

---

## [Unreleased] — 阶段四：离线分析

> 设计文档：`docs/etcd-offline-design/etcd-offline-analysis-design.md`

### Added

- **`look --snapshot` 离线数据源**：解析 bbolt snapshot db 文件，单次遍历输出全字段（当前态 KV + value size + rev_count + tombstone_count），不连集群
- **`wal-look` 命令**：解析 WAL 日志，逐条输出 WalOp（raft_index/op_type/key/value_size_bytes）；支持 `--entry-type` 过滤、`--start/end-index` 范围
- **`wal-summary` 命令**：从 WalOp JSONL 或直接解析 WAL → 按 key 聚合 Put/Delete 次数 → `--sort=put-count/delete-count` 排序 Top N
- **`dump` 命令集**：子命令 `list-bucket` / `iterate-bucket`（+ size 增强）/ `scan-keys` / `wal`，纯文本原始导出
- **`summary --sort=rev-count/tombstone-count`**：离线 snapshot JSONL 新增排序维度，定位历史 revision 堆积和频繁删除热点
- **`summary --strip-suffix=<sep>`**：分组前去掉 key 最后一段路径里、最后一个 `<sep>` 及其后的后缀，让 Kubernetes `<name>.<uid>` 类 key（events / services / endpoints…）按 `<name>` 聚合。只影响最后一段，中间段（如 `monitoring.coreos.com`）与 `--group-depth` 语义不变。默认空 = 关闭
- **`summary --sort=distinct-lease-count` + `key_leased_count` / `distinct_lease_count` / `max_lease` 列**：每个 prefix 分组三个 lease 维度：`key_leased_count` = 挂了 lease 的 key 数(可加)；`distinct_lease_count` = 不同 lease id 的数量(去重)；`max_lease` = 最大 lease id(真实可 `etcdctl lease inspect` 的 id)。`key_leased_count / distinct_lease_count` 即 lease 复用度(≈1 → 每个 key 独占 lease；>>1 → 少量 lease 被大量 key 共用)。`--sort=distinct-lease-count` 按 lease 种类数排序。与 `distribute` 的 `lease==0 = persistent` 语义一致
- **`distribute --input` / `find --input`**：消费 KeyMeta JSONL 离线分析，与 `summary --input` 对齐
- **`core/snapshot_source.go`**：`SnapshotSource` 单次遍历全字段，照抄 `ahrtr/etcd-diagnosis` 的 `BytesToBucketKey`（~60 行）
- **`core/wal_source.go`**：`WalSource` + `WalOp` 结构 + 全 12 种 entry-type
- **`core/revision.go`**：`BytesToBucketKey` + tombstone 识别

### Changed

- **`KeyMeta`** 新增 `RevCount` / `TombstoneCount` 字段，JSONL 读写适配
- **`look` help 文本**：`--keys-only` 标注"online only; ignored with --snapshot"；`--snapshot` 说明"outputs all fields in a single pass"

### Fixed

- **`summary` 表头分组数**：`Summary: X keys, Y groups` 的 `Y` 之前用的是 key 总数(如 6 个 key 分成 2 组时显示"6 groups")。现在 `Y` 是真实分组数(截断时为 `top N + M others`)

### 设计决策

- **单次遍历设计**：离线 `--snapshot` 不分 `--keys-only` 和 `--rev-count` 两种模式，一次 bbolt 遍历同时完成当前态去重 + rev_count/tombstone_count 聚合，时间复杂度不变（O(N)）
- **JSONL 管线**：`--snapshot` 只在 `look` 一个命令上，其他命令通过 `--input` 消费 JSONL（加载一次，分析多次）
- **参考实现**：核心参考 `ahrtr/etcd-diagnosis` offline（rev_count 分析），照抄不 import（v3.6.7 vs v3.5.27 版本差异）
- **依赖变更**：+ `bbolt`（快照 db）+ `server/v3`（仅 WAL 路线需要 `wal.OpenForRead`）

---

## [0.3.0] — 2026-06-23 — 阶段三：在线改造

> 提交 `34ff4e2` — 新增 summary 命令 + 共享 filter/meta 模块 + JSONL 管线 + 低风险排查流程。
>
> 改造计划详见 `docs/changelog/etcd-analysis-key-improvement-plan.md`。

### Added

- **`summary` 命令**：按前缀聚合 Top N，支持 `--group-depth` / `--sort` / `--top` / `--input`（离线 JSONL）/ `--keys-only`（在线）/ `--min/max-create/mod-revision`
- **`look --keys-only`**：在线模式底层 `WithKeysOnly()`，不拉 value，低风险快照
- **`look --write-out=jsonl`**：流式导出 KeyMeta JSONL，供 `summary --input` 离线多次分析
- **`look --prefix` / `distribute --prefix`**：server-side 前缀限定，不全量扫
- **`look --page-size` / `--page-sleep`**：扫描速率控制，降低 follower 瞬时压力
- **`core/meta.go`**：`KeyMeta` 记录 + JSONL 读写
- **`core/filter.go`**：共享 size 过滤，look/summary 复用
- **`core/summary.go`**：前缀聚合引擎 `GroupStats` / `Summarize` / `GroupPrefix`

### Changed

- **`look` size 字段拆分**：log/jsonl 输出从含义随 `--filter` 变化的单一 `size` 字段，改为固定语义的 `key_size_bytes` / `value_size_bytes` / `kv_size_bytes` / `kv_size_human`
- **`find --limit` 下推 server**：`WithLimit` 直接下推到 etcd Range，大 prefix 也安全
- **`data_source.go`**：支持 `WithKeysOnly()` / `WithPrefix()` / page-size / page-sleep

### 修改文件

| 文件 | 修改内容 |
|------|--------|
| `cmd/summary_cmd.go` | 新增 summary 命令 |
| `cmd/look_cmd.go` | keys-only / jsonl / prefix / page-size / page-sleep / filter 重构 |
| `cmd/distribute_cmd.go` | --prefix 支持 |
| `cmd/find_cmd.go` | --limit 下推 server |
| `cmd/root_cmd.go` | 注册 summary |
| `core/meta.go` | KeyMeta + JSONL 读写 |
| `core/filter.go` | 共享 FilterConfig |
| `core/summary.go` | 前缀聚合引擎 |
| `core/data_source.go` | keys-only / prefix / page 控制 |
| `core/report.go` | 适配新 helper |

---

## [0.2.0] — 2026-06-18 — 阶段二：JSON 输出功能

> 提交 `336b210` (feat) — distribute 命令新增 `--write-out=json` 支持，可通过 `--write-out=json` 输出机器可读的 JSON 格式报告，便于脚本解析和自动化处理。
>
> 提交 `e0dde72`~`a18fbf6` (fix/test) — 基于对以上两个提交的 Code Review，融合多份审查意见后执行以下修复，并建立完整测试体系。

### Added

- **`distribute --write-out=json`**：机器可读 JSON 输出（summary/histogram/percentiles）

### Fixed

- **CR-Fix 1**: `histogramJSON()` 分桶结果错误 — `sizes` 未排序导致双指针算法失效（🔴 严重）
- **CR-Fix 2**: JSON 模式空数据暴露 `math.MaxInt32` / `-1` 哨兵值
- **CR-Fix 3**: `NewReport(bc, of, true)` variadic bool → Functional Options（`WithJSONMode()`）
- **CR-Fix 4**: JSON 模式下 `processResults` 无意义 `100ms` sleep
- **CR-Fix 5**: text/json 分支数据管道重复代码 → 提取公共逻辑
- **CR-Fix 6**: `go.sum` 残留 v3.5.0 旧哈希 → `go mod tidy` 清理 219 行
- **CR-Fix 7**: `ReportJSON.Percentiles` 值单位不明确 → key 名加 `_bytes` 后缀
- **CR-Fix 8**: `percentilesJSON()` 缺 `countLock.RLock()` 保护（与 `String()` 不一致）
- **CR-Fix 9**: `String()` 中 `sort.Ints` 与 `processResult` 的 `append` 构成 data race（🔴 严重）
- **CR-Fix 10**: `processResult` 写 `Count`/`Smallest`/`Largest`/`Total`/`Average` 未持锁（🔴 严重）

### 修改文件

| 文件 | 修改内容 |
|------|--------|
| `cmd/distribute_cmd.go` | 新增 JSON 输出；使用 `WithJSONMode()`；提取 text/json 公共逻辑 |
| `core/report.go` | histogram 分桶排序；空数据保护；Functional Options；条件 sleep；单位显式化；countLock 一致性；data race 修复 |
| `go.sum` | `go mod tidy` 清理 |

> 测试体系详见 [tests/README.md](tests/README.md)。

---

## [0.1.0] — 2026-06-16 — 阶段一：初始修复

> 提交 `8751b88` — 修复 `WithPrefix` panic、flag 冲突、禁用高危命令。

### Fixed

- **Fix 1**: etcd client v3.5.0 `WithPrefix` 反射误判导致 panic — 升级 etcd client v3.5.0 → v3.5.27
- **Fix 2**: `--key` flag 冲突导致 TLS 连接失败 — `find --key` → `--match-key`，`unmarshal --key` → `--target-key`
- **Fix 3**: 禁用 `clear`（🔴 删全量数据不可逆）和 `rename`（🟠 非原子 Get→Put→Delete）高危命令

### 修改文件

| 文件 | 修改内容 |
|------|--------|
| `go.mod` | etcd client v3.5.0 → v3.5.27，go 1.18 → 1.24 |
| `go.sum` | 依赖校验和更新 |
| `cmd/root_cmd.go` | 禁用 clear 和 rename 命令 |
| `cmd/find_cmd.go` | `--key` → `--match-key` |
| `cmd/unmarsha_cmd.go` | `--key` → `--target-key` |

---

## 修复状态汇总

| # | 问题 | 严重程度 | 阶段 | 状态 |
|---|------|---------|------|------|
| 1 | `WithPrefix` 反射误判 panic | 🔴 高 | 一 | ✅ Fix 1 |
| 2 | `--key` flag 冲突致 TLS 失败 | 🔴 高 | 一 | ✅ Fix 2 |
| 3 | clear/rename 高危命令 | 🔴 高 | 一 | ✅ Fix 3 |
| 4 | `histogramJSON()` 未排序分桶错误 | 🔴 高 | 二 | ✅ CR-Fix 1 |
| 5 | JSON 空数据暴露哨兵值 | 🟡 中 | 二 | ✅ CR-Fix 2 |
| 6 | `NewReport` variadic bool API 不清晰 | 🟡 中 | 二 | ✅ CR-Fix 3 |
| 7 | JSON 模式下无意义 sleep | 🟡 中 | 二 | ✅ CR-Fix 4 |
| 8 | text/json 分支重复代码 | 🟡 中 | 二 | ✅ CR-Fix 5 |
| 9 | `go.sum` 残留旧版本哈希 | 🟢 低 | 二 | ✅ CR-Fix 6 |
| 10 | Percentiles 单位不明确 | 🟢 低 | 二 | ✅ CR-Fix 7 |
| 11 | `countLock` 保护不一致 | 🟡 中 | 二 | ✅ CR-Fix 8 |
| 12 | `String()` 中 `sort.Ints` data race | 🔴 高 | 二 | ✅ CR-Fix 9 |
| 13 | `processResult` 字段级 data race | 🔴 高 | 二 | ✅ CR-Fix 10 |
| 14 | `go 1.18 -> 1.24` 跨度较大 | 🟡 中 | — | ⏭️ 跳过（需确认 CI 环境） |
| 15 | 注释禁用命令是硬编码 | 🟡 中 | — | ⏭️ 跳过（流程建议，非代码 bug） |
