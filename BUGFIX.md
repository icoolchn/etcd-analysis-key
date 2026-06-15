# Bugfix Changelog

## Bugfix 1: etcd client v3.5.0 `WithPrefix` 误判导致 panic

### 现象

执行 `distribute`、`look`、`find`（无 prefix）命令时触发 panic：

```
panic: `WithPrefix` and `WithFromKey` cannot be set at the same time, choose one
```

### 根因

etcd client v3.5.0 使用反射 + `strings.Contains` 检测 option 类型。项目代码中 `GetDataWithPrefix` 函数名包含 `WithPrefix`，导致该函数内所有闭包名（如 `WithFromKey`、`WithSerializable`、`WithLimit`）都被误判为 `WithPrefix`，触发冲突检测。

### 修复

升级 etcd client 从 v3.5.0 到 v3.5.27。v3.5.15+ 改为在闭包内部直接设置 bool 标记，不再依赖反射和字符串匹配。

**修改文件：** `go.mod`、`go.sum`

```
go.etcd.io/etcd/api/v3        v3.5.0  → v3.5.27
go.etcd.io/etcd/client/pkg/v3 v3.5.0  → v3.5.27
go.etcd.io/etcd/client/v3     v3.5.0  → v3.5.27
go directive                   1.18   → 1.24
```

---

## Bugfix 2: `--key` Flag 冲突导致 TLS 连接失败

### 现象

使用 TLS 连接 etcd 时，`find --key=xxx` 和 `unmarshal --key=xxx` 报 TLS 握手失败：

```
tls: failed to verify certificate: x509: "etcd" certificate is not standards compliant
```

而 `leader`、`distribute`、`look` 等命令 TLS 连接正常。

### 根因

全局 PersistentFlag `--key`（TLS 私钥文件）与 find/unmarshal 子命令 LocalFlag `--key`（搜索关键字/etcd key）同名。Cobra 子命令 LocalFlag 会覆盖 PersistentFlag，导致 TLS 私钥路径被覆盖为搜索关键字字符串，证书加载失败。

```go
// 全局 flag
rootCmd.PersistentFlags().StringVar(&core.C.TLS.KeyFile, "key", "", "TLS key file")

// 子命令 flag（同名冲突！）
cmd.Flags().StringVar(&findKey, "key", "", "搜索关键字")
cmd.Flags().StringVar(&unmarshallKey, "key", "", "etcd key")
```

### 触发条件

必须同时满足：1) 使用 TLS 连接；2) 子命令使用了 `--key`

| 场景 | 结果 |
|------|------|
| 不连 TLS + `find --key=qa` | ✅ 正常（全局 --key 为空，覆盖无影响） |
| 连 TLS + `find`（不用 --key） | ✅ 正常（全局 --key 不被覆盖） |
| 连 TLS + `find --key=qa` | ❌ TLS 私钥被覆盖为 "qa"，握手失败 |

### 修复

将子命令 `--key` 重命名，避免与全局 TLS `--key` 冲突。

| 命令 | 修改前 | 修改后 |
|------|--------|--------|
| find | `--key` | `--match-key` |
| unmarshal | `--key` | `--target-key` |

命名参考项目中已有的 `--source-key`、`--target-key`、`--filter-max` 等 `<修饰>-key` 格式。

**修改文件：** `cmd/find_cmd.go`、`cmd/unmarsha_cmd.go`

**使用方式变化：**

```bash
# 修改前
etcdctl+ find --key=qa
etcdctl+ unmarshal --key=/registry/pods/default/my-pod ...

# 修改后
etcdctl+ find --match-key=qa
etcdctl+ unmarshal --target-key=/registry/pods/default/my-pod ...
```

---

## Bugfix 3: 禁用高危命令

### 原因

`clear` 和 `rename` 命令存在数据安全风险：

| 命令 | 风险等级 | 风险说明 |
|------|---------|---------|
| clear | 🔴 极高 | 删除 etcd 全部数据，不可逆 |
| rename | 🟠 高 | 非原子 Get→Put→Delete，中途失败导致数据不一致 |

### 修复

注释掉 `root_cmd.go` 中 `NewClearCmd()` 和 `NewRenameCmd()` 的注册。源码保留，取消注释即可重新启用。

**修改文件：** `cmd/root_cmd.go`

```go
// Disabled: high-risk commands that modify/delete etcd data
// rootCmd.AddCommand(NewClearCmd())   // 🔴 deletes ALL etcd data, irreversible
// rootCmd.AddCommand(NewRenameCmd())  // 🟠 non-atomic Get→Put→Delete, may cause inconsistency
```

---

## Code Review 修复 (2026-06-12)

> 基于对 `d89a6ea` (bugfix) 和 `1f1cc55` (feat) 两个提交的 Code Review，融合多份审查意见后执行以下修复。

---

### CR-Fix 1: `histogramJSON()` 分桶结果错误（严重）

**问题**

`JSON()` 方法的调用顺序：先 `histogramJSON()` 后 `percentilesJSON()`。`histogramJSON()` 的双指针分桶算法依赖 `r.stats.sizes` 已排序（指针 `bi` 只能单向前进），但此时 `sizes` 尚未排序。text 模式 `String()` 会先调用 `sort.Ints` 再调 `histogram()`，是正确的；JSON 模式顺序反了。

**修复**

在 `JSON()` 方法开头加入 `sort.Ints(r.stats.sizes)`，同时移除 `percentilesJSON()` 内的冗余排序。

**修改文件：** `core/report.go`

---

### CR-Fix 2: JSON 模式空数据暴露哨兵值

**问题**

`Count == 0` 时，`Smallest` 保持初始哨兵值 `math.MaxInt32`，`Largest` 保持 `-1`。text 模式有空数据保护（输出 `"empty data"` 并跳过 `finalString()`），但 JSON 模式会直接输出无意义的哨兵值。

**修复**

`JSON()` 开头增加空数据检查，返回合理的零值结构体：

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

**修改文件：** `core/report.go`

---

### CR-Fix 3: `NewReport` variadic bool → Functional Options

**问题**

`NewReport(bc, of, true)` 中 `true` 的含义对调用方不透明，API 设计不清晰。

**修复**

引入 Functional Options 模式：

```go
// ReportOption is a functional option for configuring a report.
type ReportOption func(*report)

// WithJSONMode enables JSON output mode (no dynamic terminal output).
func WithJSONMode() ReportOption { ... }

func NewReport(bc int, of SizeOf, opts ...ReportOption) Report { ... }
```

调用方代码变为：

```go
// JSON 模式
r := core.NewReport(bucketCount, sizeOf, core.WithJSONMode())

// Text 模式（默认）
r := core.NewReport(bucketCount, sizeOf)
```

**修改文件：** `core/report.go`、`cmd/distribute_cmd.go`

---

### CR-Fix 4: JSON 模式下 `processResults` 无意义 sleep

**问题**

`processResults()` 末尾固定 `time.Sleep(100ms)` 是为了等待 text 模式的 dynamic output 刷新。JSON 模式不需要 dynamic output，这个 sleep 纯属浪费时间。

**修复**

改为条件执行：

```go
if !r.jsonMode {
    time.Sleep(time.Millisecond * 100)
}
```

**修改文件：** `core/report.go`

---

### CR-Fix 5: `distribute_cmd.go` text/json 分支代码复用

**问题**

text 和 json 两个分支除了 `DynamicOutput()` 和最终输出方式不同外，数据管道逻辑完全重复。

**修复**

提取公共数据管道逻辑，仅在构建 Report 和最终输出处分叉：

```go
isJSON := distributeWriteOut == "json"
var r core.Report
if isJSON {
    r = core.NewReport(bucketCount, sizeOf, core.WithJSONMode())
} else {
    r = core.NewReport(bucketCount, sizeOf)
}
// 公共数据管道
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

**修改文件：** `cmd/distribute_cmd.go`

---

### CR-Fix 6: `go.sum` 残留旧版本哈希

**问题**

升级 etcd client 后 `go.sum` 中同时保留了 v3.5.0 和 v3.5.27 两套哈希。

**修复**

执行 `go mod tidy`，清理 219 行残留旧哈希。

**修改文件：** `go.sum`

---

## 修改文件汇总

| 文件 | 修改内容 |
|------|--------|
| `cmd/root_cmd.go` | 禁用 clear 和 rename 命令 |
| `cmd/find_cmd.go` | `--key` → `--match-key` |
| `cmd/unmarsha_cmd.go` | `--key` → `--target-key` |
| `go.mod` | etcd client v3.5.0 → v3.5.27，go 1.18 → 1.24 |
| `go.sum` | 依赖校验和更新；`go mod tidy` 清理 219 行旧哈希 |
| `core/report.go` | 修复 `histogramJSON()` 分桶排序；JSON 空数据保护；`NewReport` 改为 functional options；JSON 模式跳过无用 sleep |
| `cmd/distribute_cmd.go` | 使用 `core.WithJSONMode()` 替代 `true`；提取 text/json 公共逻辑 |
