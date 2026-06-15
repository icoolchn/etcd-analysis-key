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

## 修改文件汇总

| 文件 | 修改内容 |
|------|---------|
| `cmd/root_cmd.go` | 禁用 clear 和 rename 命令 |
| `cmd/find_cmd.go` | `--key` → `--match-key` |
| `cmd/unmarsha_cmd.go` | `--key` → `--target-key` |
| `go.mod` | etcd client v3.5.0 → v3.5.27，go 1.18 → 1.24 |
| `go.sum` | 依赖校验和更新 |
