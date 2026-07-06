# etcd-analysis 项目分析

## 一、项目定位

etcd-analysis 是一个 **etcd 数据分析 CLI 工具**（二进制名 `etcdctl+`），核心解决的问题是：**etcd 中存储的数据大小分布不可见**。etcd 对 key-value 大小敏感，大 value 会影响 watch 稳定性和内存占用，这个工具让运维/测试人员能直观地看到 etcd 中数据的大小分布、快速检索和解码数据。

> etcd 通常用于存储系统元数据或服务发现，适合存储小型键值对。同时 etcd 对键值对的大小很敏感，存储大键值对时，如果数量过多，会带来很多不良影响，例如 watch 功能的稳定性降低，以及占用大量内存。

项目来源：https://github.com/SimFG/etcd-analysis（上游，已归档）
内部 Fix 版：http://git.17usoft.com/middleware_sre/etcd-analysis-key.git（Company GitLab）

---

## 二、整体架构

```
┌─────────────────────────────────────────────────────┐
│                      main.go                        │
│                    cmd.Start()                      │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                   cmd/ (Cobra CLI)                  │
│  root_cmd.go ─── 全局 flag (endpoints, TLS, timeout)│
│      ├── distribute_cmd.go   数据大小分布分析        │
│      ├── look_cmd.go         查看/导出全量数据       │
│      │                       + --snapshot 离线入口   │
│      ├── find_cmd.go         按关键字搜索 key        │
│      ├── summary_cmd.go      按前缀聚合 Top N        │
│      ├── leader_cmd.go       查询 leader 节点信息    │
│      ├── clear_cmd.go        清空所有数据（已禁用）  │
│      ├── decode_cmd.go       Base64 解码            │
│      ├── rename_cmd.go       重命名 key（已禁用）    │
│      ├── unmarshal_cmd.go    Proto 反序列化          │
│      ├── wal_look_cmd.go     WAL 操作流导出          │ ← 离线新增
│      ├── wal_summary_cmd.go  WAL 按 key 聚合统计     │ ← 离线新增
│      └── dump_cmd.go         原始数据导出            │ ← 离线新增
│           ├── list-bucket / iterate-bucket / scan-keys│
│           └── wal                                     │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                  core/ (核心层)                      │
│  config.go      ─── 全局配置 Cfg (endpoints/TLS)     │
│  data_source.go ─── etcd 客户端初始化 + 流式数据读取  │
│  etcd_op.go     ─── etcd CRUD 操作封装 (带超时)      │
│  report.go      ─── 统计报表引擎 (直方图/百分位)      │
│  percent.go     ─── 百分位数计算 (P10~P99)           │
│  meta.go        ─── KeyMeta 记录 + jsonl 读写 + log  │
│  filter.go      ─── 共享 size 过滤 (look/summary)    │
│  summary.go     ─── 前缀聚合引擎 (GroupStats/Sort)   │
│  util.go        ─── 通用工具函数                     │
│  snapshot_source.go ─ 快照 db 离线解析 (bbolt)       │ ← 离线新增
│  wal_source.go  ─── WAL 离线解析 + WalOp            │ ← 离线新增
│  revision.go    ─── BytesToBucketKey + tombstone     │ ← 离线新增
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│            etcd client/v3 (官方 SDK)                │
│         protoreflect (proto 动态解析)               │
│            uilive (终端实时刷新输出)                  │
│            bbolt (快照 db 只读解析)                  │ ← 离线新增
└─────────────────────────────────────────────────────┘
```

---

## 三、核心模块详解

### 3.1 `core/config.go` — 全局配置

```go
type Cfg struct {
    Endpoints      []string           // etcd 集群地址
    TLS            transport.TLSInfo  // TLS 证书配置
    CommandTimeout int                // 操作超时(秒)
}
```

全局单例 `C`，由 root_cmd 的 `PersistentFlags` 注入，所有子命令共享。

### 3.2 `core/data_source.go` — 数据获取引擎

**原理：流式分页读取**

```go
func GetDataWithPrefix(prefix string) (*clientv3.GetResponse, <-chan []*mvccpb.KeyValue)
```

这是整个项目最核心的数据管道设计：

- **带 prefix 调用**：单次 `Get(prefix, WithPrefix())` 拿全量，直接返回
- **无 prefix（全量读取）**：采用**分页游标迭代**模式
  1. 第一次 Get 取前 2 条（探测是否有数据）
  2. 启动 goroutine，以最后一条 key 为游标，每次读取 `readCount=1000` 条
  3. 通过 channel `chan []*mvccpb.KeyValue` 流式推送给消费方
  4. 当返回 ≤1 条时结束

关键设计点：

- 使用 `WithSerializable()` 选项——读请求走 serializable 模式，不经过 Raft 共识，降低对集群的压力
- 随机选择 endpoint 分散读请求压力：`connEndpoints = []string{C.Endpoints[rand.Intn(l)]}`

### 3.3 `core/etcd_op.go` — etcd 操作封装

对 `clientv3` 的 Put/Get/Delete/Txn/Status 做了统一超时封装，所有操作使用 `context.WithTimeout` 保证不会无限阻塞。

### 3.4 `core/report.go` — 统计报表引擎

**这是 distribute 命令的核心，实现了一个流式统计+实时渲染的报表系统。**

```go
type Report interface {
    Results() chan<- []*mvccpb.KeyValue  // 数据输入通道
    Run() <-chan string                   // 启动处理，返回完成信号
    DynamicOutput()                       // 实时刷新终端输出
}
```

**工作流程：**

```
etcd 数据流 ──→ results channel ──→ processResults() ──→ 统计 Stats
                                                        │
                                       DynamicOutput() ←─┘ 每 100ms 刷新终端
                                                        │
                                       processOver ──→ finalString() 输出最终结果
```

**Stats 结构：** 记录 Count/Total/Smallest/Largest/Average + `sizeToCount map[int]int` 用于直方图和百分位计算。

**`SizeOf` 函数**：由命令行 `--type` 参数决定统计维度——key 大小、value 大小、或 kv 合计大小。

**直方图算法**：将 [Smallest, Largest] 均分为 bucketCount 个桶，统计每个桶的计数，归一化到最大 40 个 `∎` 字符。

### 3.5 `core/percent.go` — 百分位数计算

计算 P10/P25/P50/P75/P90/P95/P99，遍历排序后的 sizes 数组，根据累计计数比例确定分位点。

---

## 四、子命令原理

| 命令 | 功能 | 核心原理 |
|------|------|---------|
| **distribute** | 数据大小分布分析 | 流式读取 → Report 统计 → 直方图 + 百分位实时渲染；支持 `--prefix` 限定范围 |
| **look** | 查看/导出全量数据 | 在线：流式读取 → 表格/log/jsonl 输出；`--keys-only` 不拉 value，`--prefix` server-side 限定，`--filter` 客户端过滤，hang 模式定时刷新。离线：`--snapshot` 解析 bbolt db，一次遍历输出全字段（含 rev_count/tombstone_count） |
| **find** | 按关键字搜索 key | 流式读取 → `strings.Contains` 匹配；`--limit` 下推到 etcd server（`WithLimit`），大 prefix 安全 |
| **summary** | 按前缀聚合 Top N | 在线扫描或读 `--input` JSONL 快照 → 按 `--group-depth` 聚合 → `--sort` 排序输出 Top N；支持 revision 过滤 |
| **leader** | 查询 leader 节点 | `MemberList` + `Status` 获取 leader ID → 匹配 member 信息 |
| **clear** | 清空所有数据（已禁用） | 先 `WithCountOnly` 统计数量 → 确认 → `Delete(WithFromKey)` 全删 |
| **decode** | Base64 解码 | 纯本地 `base64.StdEncoding.DecodeString`，不需要连接 etcd |
| **rename** | 重命名 key（已禁用） | Get 旧值 → Put 新 key → 可选备份到 `etcd-bak/` 前缀 → Delete 旧 key |
| **unmarshal** | Proto 反序列化 | Get 原始字节 → `protoparse` 解析 .proto 源文件 → `dynamic.Message` 动态反序列化 → 打印字段 |
| **wal-look** | WAL 操作流导出（离线） | 解析 WAL 日志 → 逐条输出 WalOp（raft_index/op_type/key/value_size_bytes）；支持 `--entry-type` 过滤、`--start/end-index` 范围 |
| **wal-summary** | WAL 按 key 聚合统计（离线） | 从 WalOp JSONL 或直接解析 WAL → 按 key 聚合 Put/Delete 次数 → `--sort=put-count/delete-count` 排序 Top N |
| **dump** | 原始数据导出（离线） | 子命令集：`list-bucket`（列 bucket）、`iterate-bucket`（逐条导出 + size 增强）、`scan-keys`（按 revision 扫描）、`wal`（WAL 条目原始输出）。纯文本，不参与 JSONL 分析管线 |

**unmarshal 命令是最有特色的**——它不需要编译 proto 代码，只需要提供 `.proto` 源文件路径，利用 `jhump/protoreflect` 做动态解析和反序列化，解决了"etcd 中存的是 protobuf 序列化数据，无法直接阅读"的痛点。

---

## 五、数据流全景

```
用户输入 CLI 命令
       │
       ▼
  root_cmd 解析全局 flag (endpoints/TLS/timeout)
       │
       ▼
  子命令 Run 函数
       │
       ├─── 在线路径 ──────────────────────────────────────────────
       │    ├── core.InitClient()  ──→  建立 etcd v3 client 连接 (随机选 endpoint)
       │    │                            └── EtcdStatus() 健康检查
       │    │
       │    ├── core.GetAllData() / GetDataWithPrefix()  ──→  流式分页读取
       │    │       │
       │    │       └── chan []*mvccpb.KeyValue  (生产者-消费者模式)
       │    │
       │    └── 业务逻辑处理
       │           ├── distribute: 喂入 Report → 统计 → 直方图/百分位
       │           ├── look:       过滤 → 格式化表格/log/jsonl → 写 stdout/file
       │           ├── find:       strings.Contains 匹配 → 输出（limit 下推 server）
       │           ├── summary:    KeyMeta 聚合 → GroupStats → Top N 排序输出
       │           ├── unmarshal:  protoreflect 动态解析 → 打印结构化字段
       │           └── 其他:       直接调用 etcd_op 封装
       │
       ├─── 离线路径（snapshot db）─────────────────────────────────
       │    ├── look --snapshot:  core.SnapshotSource(dbPath)
       │    │       │              └── 只读 bbolt → Unmarshal → 去重 + rev_count 聚合
       │    │       └── chan []*mvccpb.KeyValue（同在线，消费层零改动）
       │    │
       │    └── JSONL 管线（加载一次，分析多次）
       │           look --snapshot → KeyMeta JSONL → summary/distribute/find --input
       │
       ├─── 离线路径（WAL）─────────────────────────────────────────
       │    ├── wal-look:   core.WalSource(dataDir) → chan WalOp → JSONL
       │    └── wal-summary: 读 WalOp JSONL → 按 key 聚合 Put/Delete 次数
       │
       └─── 离线路径（dump）────────────────────────────────────────
            └── dump:  list-bucket / iterate-bucket / scan-keys / wal → 纯文本原始导出
```

---

## 六、设计亮点与不足

**亮点：**

1. **流式分页读取**：全量读取时不一次性加载到内存，用 channel 做生产者-消费者解耦，适合大数据量场景
2. **Serializable 读**：`WithSerializable()` 降低读请求对 Raft 提议管道的压力，减少对在线业务的影响
3. **实时终端渲染**：利用 `uilive` 库在 distribute 命令中每 100ms 刷新输出，用户体验好
4. **Proto 动态反序列化**：unmarshal 命令不需要编译目标 proto，直接用源文件解析，实用性很强
5. **随机 endpoint**：分散读压力，避免单节点过载
6. **keys-only + prefix + summary 三段式低风险排查**：改造后优先 keys-only 快照、server-side prefix/limit 下推、summary 聚合，只对可疑前缀拉 value，显著降低生产 follower 读取风险
7. **JSONL 快照 + 离线多次分析**：`look --write-out=jsonl` 一次导出，`summary --input` 反复多维分析，避免重复扫描 etcd

**不足：**

1. **无认证支持**：不支持 etcd 的 user/password 认证，只能连无认证或 TLS 认证的集群
2. **全局变量较多**：`core` 包大量使用包级全局变量（`client`、`C`），不利于测试和并发
3. **rename 非原子**：rename 操作是 Get→Put→Delete 三步，非事务操作，中途失败会留下不一致状态（已禁用该命令）
4. **clear 无 prefix 选项**：只能清空全部数据，无法按前缀清理（已禁用该命令）
5. **错误处理粗暴**：大量 `core.Exit(err)` 直接 `os.Exit(-1)`，没有优雅的资源清理
6. **TLS 证书验证默认跳过**：配 TLS 即 `InsecureSkipVerify=true`，存在中间人风险；计划改为显式 `--insecure-skip-tls-verify` flag，尚未落地
7. **endpoint 选择仍为随机**：未提供 `--endpoint-pick=first|random`，排查时需手动只传一个 follower endpoint

---

## 七、使用指南

### 7.1 安装

```bash
# 克隆仓库
git clone http://git.17usoft.com/middleware_sre/etcd-analysis-key.git
cd etcd-analysis-key

# 编译
go build -o etcdctl+
```

编译后得到可执行文件 `etcdctl+`。

### 7.2 全局参数

所有子命令共享以下参数（由 `root_cmd.go` 定义）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--endpoints` | `127.0.0.1:2379` | etcd 集群地址，多个用逗号分隔（排查生产建议只传一个 follower，避免随机选到 leader） |
| `--cert` | 空 | TLS 客户端证书文件 |
| `--key` | 空 | TLS 客户端私钥文件 |
| `--cacert` | 空 | TLS CA 证书文件 |
| `--command-timeout` | `5` | 操作超时时间（秒） |

> ⚠️ **TLS 证书验证现状：** 当前一旦配置 `--cert`/`--key`/`--cacert`，客户端会无条件设置 `InsecureSkipVerify=true`，跳过服务端证书验证。自签证书 / 主机名不匹配场景下可用，但存在中间人风险，跨环境（QA/生产）使用时需注意。后续计划改为显式 `--insecure-skip-tls-verify` flag（默认严格验证），目前尚未落地。

```bash
# 连接远程 etcd
etcdctl+ --endpoints=10.0.0.1:2379,10.0.0.2:2379 distribute

# 连接 TLS 加密的 etcd
etcdctl+ --endpoints=https://10.0.0.1:2379 \
  --cert=/etc/etcd/cert.pem \
  --key=/etc/etcd/key.pem \
  --cacert=/etc/etcd/ca.pem \
  distribute
```

### 7.3 `distribute` — 数据大小分布分析

**最核心的命令**，直观展示 etcd 中数据的大小分布。

```bash
# 默认按 key 大小分布，5 个桶
etcdctl+ distribute

# 按 value 大小分布，8 个桶
etcdctl+ distribute --type=value --bucket=8

# 按 key+value 合计大小分布
etcdctl+ distribute --type=kv

# 离线模式：从 JSONL 快照分析（不连 etcd）
etcdctl+ distribute --input=snapshot.jsonl --type=kv
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--type` | `key` | 统计维度：`key` / `value` / `kv` |
| `--bucket` | `5` | 直方图桶数 |
| `--write-out` | `text` | 输出格式：`text` / `json` |
| `--input` | 空 | JSONL 快照文件（离线模式）；空=在线扫描。消费 `look --write-out=jsonl` 或 `look --snapshot --write-out=jsonl` 导出的 KeyMeta JSONL |
| `--prefix` | 空 | server-side 前缀扫描，不再只能全量扫（仅在线模式） |
| `--page-size` | `1000` | 每页读取 key 数；低峰期可调大减少 Range 次数（仅在线模式） |
| `--page-sleep` | `0` | 页间 sleep（如 `50ms`），降低对 follower 瞬时压力（仅在线模式） |

**输出示例：**

```
Summary:
  Count:        116.
  Total:        7.3 KiB.
  Smallest:     22.0 B.
  Largest:      85.0 B.
  Average:      64.0 B.

Size histogram:
  22.0 B [1]    |
  34.0 B [6]    |∎∎∎
  46.0 B [29]   |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
  58.0 B [13]   |∎∎∎∎∎∎∎
  70.0 B [1]    |
  85.0 B [66]   |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎

Size distribution:
  10% in 38.0 B.
  25% in 39.0 B.
  50% in 76.0 B.
  75% in 83.0 B.
  90% in 85.0 B.
```

**解读：**

- Summary：总数/总大小/最小/最大/平均
- 直方图：每个桶的区间大小和数量，`∎` 越多代表该区间的数据越多
- 百分位分布：P50=76B 意味着一半的 key 大小不超过 76B

**实际用途：** 快速发现是否有异常大的 value 挤压 etcd，影响 watch 和内存。

#### 7.3.1 输出字段含义

**Summary 字段：**

| 字段 | 含义 | 示例解读 |
|------|------|---------|
| `Count` | 数据总条数 | etcd 中共有 116 条 KV |
| `Total` | 数据总大小 | 所有 KV 合计 7.3 KiB |
| `Smallest` | 最小的那条的字节数 | 最小占 22 字节 |
| `Largest` | 最大的那条的字节数 | 最大占 85 字节 |
| `Average` | 所有数据的平均字节数 | 平均每条占 64 字节 |

**百分位字段：**

**P50 = 76 表示：50% 的数据大小 ≤ 76 字节**

| 百分位 | 示例值 | 含义 |
|--------|--------|------|
| P10 | 38B | 10% 的数据 ≤ 38B，90% 的数据 > 38B |
| P25 | 39B | 25% 的数据 ≤ 39B |
| **P50** | **76B** | **50% 的数据 ≤ 76B（中位数）** |
| P75 | 83B | 75% 的数据 ≤ 83B |
| P90 | 85B | 90% 的数据 ≤ 85B，10% > 85B |
| P95 | 85B | 95% 的数据 ≤ 85B，5% > 85B |
| P99 | 85B | 99% 的数据 ≤ 85B，1% > 85B |

**简单记：** P50 看整体水平，P99 看极端情况。百分位越大，越能发现尾部的大数据。

#### 7.3.2 JSON 输出

`distribute` 支持 `--write-out=json` 输出 JSON 格式，方便程序调用和自动化集成。

```bash
# JSON 格式输出
etcdctl+ distribute --type=value --write-out=json

# 文本格式输出（默认）
etcdctl+ distribute --type=value
etcdctl+ distribute --type=value --write-out=text
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--write-out` | `text` | 输出格式：`text` / `json` |

**JSON 输出示例：**

```json
{
  "summary": {
    "count": 116,
    "total_bytes": 7475,
    "smallest_bytes": 22,
    "largest_bytes": 85,
    "average_bytes": 64
  },
  "histogram": [
    {"bucket_start": 22, "bucket_end": 34, "count": 1},
    {"bucket_start": 34, "bucket_end": 46, "count": 6},
    {"bucket_start": 46, "bucket_end": 58, "count": 29},
    {"bucket_start": 58, "bucket_end": 70, "count": 13},
    {"bucket_start": 70, "bucket_end": 85, "count": 66}
  ],
  "percentiles": {
    "p10": 38, "p25": 39, "p50": 76, "p75": 83, "p90": 85, "p95": 85, "p99": 85
  }
}
```

**JSON 字段说明：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `summary.count` | int | KV 总条数 |
| `summary.total_bytes` | int | KV 总字节数 |
| `summary.smallest_bytes` | int | 最小条目字节数 |
| `summary.largest_bytes` | int | 最大条目字节数 |
| `summary.average_bytes` | int | 平均字节数 |
| `histogram[].bucket_start` | int | 桶起始字节数 |
| `histogram[].bucket_end` | int | 桶结束字节数 |
| `histogram[].count` | int | 桶内条目数 |
| `percentiles.p10~p99` | int | 百分位字节数 |

**用途：**

```bash
# 程序调用，获取数据条数
etcdctl+ distribute --type=value --write-out=json | jq '.summary.count'

# 告警判断：最大 value 是否超过 1MB
largest=$(etcdctl+ distribute --type=value --write-out=json | jq '.summary.largest_bytes')
if [ $largest -gt 1048576 ]; then echo "ALERT: value > 1MB"; fi

# 写入监控系统
etcdctl+ distribute --type=kv --write-out=json | curl -X POST http://monitor/api/etcd-stats -d @-
```

**修改文件：** `cmd/distribute_cmd.go`、`core/report.go`

#### 7.3.3 etcd 巡检数据大小参考阈值

etcd 官方建议单个 value 不超过 1.5MB，总数据量建议不超过 8GB。结合实际运维经验，建议巡检阈值如下：

| 指标 | 建议阈值 | 说明 |
|------|---------|------|
| **单个 value** | ≤ **1 MB** | 超过 1MB 影响_watch_性能和 MVCC 内存 |
| 单个 value 危险线 | > 1.5 MB | etcd 官方硬限制，超过会严重影响稳定性 |
| **P99 value** | ≤ **256 KiB** | 1% 的尾部数据也不应过大 |
| **总数据量** | ≤ **2 GB** | 超过后 defrag 和 snapshot 时间显著增加 |
| 总数据量危险线 | > 8 GB | 官方建议上限，超过影响启动恢复时间 |
| **KV 总条数** | ≤ **200 万** | 条数过多影响 range 查询和 watch 延迟 |

**巡检查询命令：**

```bash
# 1. 查看 value 大小分布，关注 Largest 和 P99
etcdctl+ distribute --type=value --bucket=10

# 2. 查看 KV 合计大小，关注 Total
etcdctl+ distribute --type=kv --bucket=10

# 3. 定位大 value（超过 1MB 的 key）
etcdctl+ look --filter=value --filter-min=1048576 --show-value
```

**巡检频率建议：**

| 场景 | 频率 |
|------|------|
| 生产集群 | 每日一次，非高峰期执行 |
| 新服务上线 | 上线后立即检查，观察数据增长趋势 |
| 出现 watch 延迟 | 立即检查，重点看大 value |

### 7.4 `look` — 查看/导出全量数据

```bash
# 终端查看（默认不显示 value）
etcdctl+ look

# 显示 value（base64 编码）
etcdctl+ look --show-value

# 导出到文件 analysis.txt
etcdctl+ look --write-out=file

# 持续监听，每 2 秒刷新一次（仅 file 模式生效）
etcdctl+ look --write-out=file --hang=true --hang-interval=5

# 只看 value 大小在 74~100 字节之间的数据（客户端过滤，仍会拉 value）
etcdctl+ look --filter=key --filter-min=74 --filter-max=100

# 输出为日志格式（适合 loki 等日志系统采集）,"log" 和 "file" 均是文件写入
etcdctl+ look --write-out=log

# 只扫某个前缀（server-side，不全量扫）
etcdctl+ look --prefix=/registry/events --write-out=log

# keys-only：只拉 key metadata，不拉 value（低风险快照）
etcdctl+ look --keys-only --write-out=log

# 导出 keys-only JSONL 快照，供 summary --input 离线反复分析
etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl

# 导出某前缀 full metadata JSONL（含 value size，不含 value 内容）
etcdctl+ look --prefix=/registry/events --write-out=jsonl --output=events-kv-meta.jsonl

# 离线 snapshot db 分析：一次遍历输出全字段（含 rev_count + tombstone_count）
etcdctl+ look --snapshot=cluster.db --write-out=jsonl --output=snapshot.jsonl

# 配合管道使用
etcdctl+ look | more
etcdctl+ look | vim -
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--show-value` | `false` | 是否显示 value（base64 编码） |
| `--write-out` | `stdout` | 输出方式：`stdout` / `file` / `log` / `jsonl` |
| `--output` | 空 | `file`/`log`/`jsonl` 写入的文件路径（默认 `analysis.txt` / `analysis.jsonl`） |
| `--hang` | `false` | 持续刷新（需配合 `--write-out=file`，仅在线模式） |
| `--hang-interval` | `2` | 刷新间隔（秒） |
| `--filter` | `none` | 大小过滤维度：`none` / `key` / `value` / `kv`（客户端过滤，仍会拉 value） |
| `--filter-min` | `-1` | 最小值（字节） |
| `--filter-max` | `-1` | 最大值（字节） |
| `--keys-only` | `false` | 只拉 key metadata，不拉 value（底层 `WithKeysOnly()`），低风险快照。仅在线模式生效；离线 `--snapshot` 模式静默忽略（详见 8.2 keys-only 说明） |
| `--snapshot` | 空 | 离线模式：解析 bbolt snapshot db 文件。一次遍历输出全字段（含 value size + rev_count + tombstone_count），不连集群 |
| `--prefix` | 空 | server-side 前缀扫描，限定范围（仅在线模式） |
| `--page-size` | `1000` | 每页读取 key 数（仅在线模式） |
| `--page-sleep` | `0` | 页间 sleep（如 `50ms`）（仅在线模式） |

**输出示例（stdout 模式）：**

```
Current Stage
  cluster_id:2037210783374497686 member_id:13195394291058371180 revision:254946 raft_term:9
Kv List
| Key | Value | Size | CreateRevision | ModRevision | Version | Lease |
| /config/server | - | 85.0 B | 1 | 1 | 1 | 0 |
```

**输出示例（log 模式，拆分 size 字段后）：**

```
key=/config/server value=- key_size_bytes=13 value_size_bytes=0 kv_size_bytes=13 kv_size_human=13B create_revision=1 mod_revision=1 version=1 lease=0
```

**输出示例（keys-only + log 模式，省略 value 相关字段）：**

```
key=/config/server key_size_bytes=13 create_revision=1 mod_revision=1 version=1 lease=0
```

> 📌 **size 字段已拆分：** 早期 `size` 字段含义会随 `--filter` 改变（key/value/kv），对离线脚本不友好。改造后 log/jsonl 统一输出 `key_size_bytes` / `value_size_bytes` / `kv_size_bytes` / `kv_size_human`，含义固定，不再随 filter 变化。keys-only 模式下 value 相关字段省略。

#### 7.4.1 输出字段含义

look 命令每行输出的 7 个字段（对应 etcd 内部 mvcc 的 KV 结构，也可通过 `etcdctl get <key> -w json` 得到），逐字段含义如下：

| 字段 | 示例值 | 含义 |
|------|--------|------|
| `key` | `/qaenv` | 这个键的名字 |
| `value` | `-` | 该键对应的值。`-` 是 look 表格/log 输出对“空 value”的占位符，实际 value 为空字节串，不是字符串 `"-"` |
| `size` | `6.0B` | value 的存储大小（human-readable 显示；`--filter` 过滤的也是这个 size） |
| `create_revision` | `35630` | 该 key **首次被创建**时的全局修订号（revision）。只要这个 key 没被删除再重建，此值不变 |
| `mod_revision` | `35630` | 该 key **最后一次被修改**时的全局修订号，每次 PUT 都会更新。等于 `create_revision` 说明自创建后再没被修改过 |
| `version` | `1` | 该 key 自最近一次创建以来被**修改的次数**。`1` 表示只创建、未更新过（每改一次 +1；删除重建后重置为 1） |
| `lease` | `0` | 关联的租约 ID。`0` 表示**没有绑定 lease**，即这个 key 是持久化的，不会因某个租约过期而被自动删除 |

**三个 revision/version 字段最容易混淆，区别如下：**

- **`create_revision`**：key 第一次被写入时集群的全局 revision，跟着 key 的“生命周期”走，删除后重建会变。
- **`mod_revision`**：key 最近一次被改写时集群的全局 revision，每次 PUT 都会更新。
- **`version`**：从这次创建算起，被改写了几次。

三者关系：当 `create_revision == mod_revision` 且 `version == 1` 时，可以判定这个 key 是“一次性写入后从未被修改”的——上面那条 `/config/server` 正是这种情况。

**关于 `value=-` 和 `size`：**

- `value=-` 在 look 输出里代表空 value，真实 value 为空字节串。
- `size` 是 value 的字节数（表格/log 模式下人性化显示带单位）。注意 etcd 对单个 value 的存储还会包含 key + 元数据开销，但 look 显示的 `size` 通常指 value 本身大小。

**一句话总结：** 上面这行表示 `/qaenv` 是一个**值为空、创建后从未修改过（version=1）、未绑定租约（持久存储）**的普通键，它是在集群全局 revision 为 `35630` 时被写入的。

⚠️ 性能提示： look 会全量读取 etcd 所有数据，数据量大时对集群读压力较大。建议先用 distribute --type=kv 查看数据总量和条数，数据量大时（几十万条+）谨慎使用 look，避免对生产集群造成过大读压力。

### 7.5 `find` — 按关键字搜索 key

```bash
# 按前缀搜索
etcdctl+ find --prefix=/registry/pods

# 搜索并显示 value
etcdctl+ find --match-key=index --value

# 限制返回数量
etcdctl+ find --match-key=index --limit=50

# 离线模式：从 JSONL 快照搜索（不连 etcd）
etcdctl+ find --input=snapshot.jsonl --match-key=starrocks
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--match-key` | 空 | 搜索关键字（包含匹配） |
| `--prefix` | 空 | key 前缀过滤 |
| `--value` | `false` | 是否显示 value |
| `--limit` | `10` | 最大返回数量（在线模式下推到 etcd server 作为 Range `WithLimit`，大 prefix 也安全） |
| `--input` | 空 | JSONL 快照文件（离线模式）；空=在线扫描。消费 `look --write-out=jsonl` 或 `look --snapshot --write-out=jsonl` 导出的 KeyMeta JSONL |

> ⚠️ **Breaking Change：** 修复前版本使用 `--key`，与全局 TLS `--key` 冲突，TLS 环境下会导致连接失败。
> - 修复前：`etcdctl+ find --key=index`（TLS 环境下有 bug）
> - 修复后：`etcdctl+ find --match-key=index`

### 7.6 `summary` — 按前缀聚合 Top N

**改造新增的核心分析命令**，把全量 key 按前缀分组聚合，只输出 Top N，避免导出全量明细。支持两种模式：

- **在线模式**：不传 `--input`，直接连 etcd 扫描（建议加 `--keys-only` + `--prefix`）。
- **离线模式**：传 `--input=<jsonl>`，从 `look --write-out=jsonl` 导出的快照分析，不访问 etcd，可对同一份快照反复多维分析。

```bash
# 在线便捷模式：keys-only 聚合 key 数最多的前缀
etcdctl+ summary --keys-only --group-depth=2 --sort=count --top=50

# 离线模式：基于 keys-only 快照反复分析
etcdctl+ summary --input=keys.jsonl --group-depth=2 --sort=count --top=50
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=max-version --top=50

# 某时间点后修改/新增最多的前缀（revision 来自 Grafana）
etcdctl+ summary --input=keys.jsonl --min-mod-revision=38770000000 --group-depth=3 --sort=count --top=50
etcdctl+ summary --input=keys.jsonl --min-create-revision=38770000000 --group-depth=3 --sort=count --top=50

# 可疑前缀空间占用（需含 value size 的快照）
etcdctl+ summary --input=events-kv-meta.jsonl --group-depth=3 --sort=total-size --top=50

# JSON 输出
etcdctl+ summary --input=keys.jsonl --sort=count --write-out=json --output=summary.json
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--input` | 空 | JSONL 快照文件（离线模式）；空=在线扫描 |
| `--keys-only` | `false` | 在线模式只拉 key metadata，不拉 value |
| `--prefix` | 空 | 在线模式 server-side 前缀扫描 |
| `--group-depth` | `2` | 按前 N 段路径聚合 |
| `--strip-suffix` | 空 | 分组前去掉最后一段路径里、最后一个 `<sep>` 及其后的后缀。如 `.` 让 K8s `<name>.<uid>` key 按 `<name>` 聚合；只动最后一段，中间段与 depth 语义不变 |
| `--top` | `20` | 输出前 N 个分组（不是 N 个 key） |
| `--sort` | `count` | 排序键，见下表 |
| `--min-create-revision` / `--max-create-revision` | `0` | create_revision 范围，影响 `created-count` |
| `--min-mod-revision` / `--max-mod-revision` | `0` | mod_revision 范围，影响 `modified-count` |
| `--filter` / `--filter-min` / `--filter-max` | `none` / `-1` / `-1` | 客户端过滤（不减少 server 返回量） |
| `--page-size` / `--page-sleep` | `1000` / `0` | 仅在线模式 |
| `--write-out` | `text` | `text` / `json` |
| `--output` | 空 | 写文件（默认 stdout） |

**`--group-depth` 分组规则**（以 `/registry/pods/default/nginx` 为例）：

| depth | 分组前缀 |
|---|---|
| 1 | `/registry` |
| 2 | `/registry/pods` |
| 3 | `/registry/pods/default` |
| 4 | `/registry/pods/default/nginx` |

K8s 场景建议：`--group-depth=2` 看资源类型，`--group-depth=3` 看资源类型 + namespace。

| `--sort` 取值：

| sort | 用途 |
|---|---|
| `count` | key 数最多的前缀 |
| `total-size` / `avg-size` / `max-size` | 占用最大 / 平均大 / 单个大对象（需含 value 的快照） |
| `max-version` | 高频覆盖写热点前缀 |
| `latest-mod-revision` | 最近活跃修改的前缀 |
| `created-count` / `modified-count` | 配合 `--min-*-revision` 找某 revision 后新增/修改最多的前缀 |
| `distinct-lease-count` | 组内不同 lease id 数最多的前缀（lease 种类最杂） |
| `rev-count` | 历史 revision 数最多的前缀（离线 snapshot JSONL；在线快照无此字段） |
| `tombstone-count` | tombstone 数最多的前缀（离线 snapshot JSONL；在线快照无此字段） |

**lease 三列**（常驻显示，与 `distribute` 的 `lease==0 = persistent` 语义一致）：`key_leased_count`（挂 lease 的 key 数，可加）/ `distinct_lease_count`（不同 lease id 数，不可加）/ `max_lease`（最大 lease id，真实可 `etcdctl lease inspect`，不可加）。`key_leased_count / distinct_lease_count` 即 lease 复用度：≈1 → 每个 key 独占 lease；>>1 → 少量 lease 被大量 key 共用。others 行：`key_leased_count` 求和，`distinct_lease_count`/`max_lease` 显示 `-`。

**输出示例（text）：**

```
Summary: 2090000 keys, 15 groups (top 15 by count)

prefix | count | total_size | avg_size | max_size | max_version | latest_mod_revision | created_count | modified_count | key_leased_count | distinct_lease_count | max_lease | rev_count | tombstone_count | percent
/registry/events | 980000 | - | - | - | 3 | 38780000020 | 0 | 0 | 980000 | 980000 | 5821917203594880612 | 980000 | 0 | 99.0%
/registry/pods | 530000 | - | - | - | 1024 | 38780000010 | 0 | 0 | 0 | 0 | 0 | 530000 | 0 | 0.5%
...
```

keys-only 快照没有 value size，`TotalSize`/`AvgSize`/`MaxSize` 显示 `-`；`rev_count`/`tombstone_count` 仅离线 snapshot JSONL 有值（在线/普通 keys-only 快照这两列不显示）。用 `look --prefix=... --write-out=jsonl`（不带 `--keys-only`）导出的快照才有 size 列。

**`--strip-suffix` 用法**：K8s `<name>.<uid>` 类 key（events/services/endpoints…）默认 `--group-depth=4` 会因 `<uid>` 唯一而每个 key 各成一组。加 `--strip-suffix=.` 剥掉最后一段的 `.<uid>`，同一 `<name>` 的 key 才会合并：

```bash
# kyuubi events 按事件系列名聚合（同 name 的多个 event 合并）
etcdctl+ summary --input=keys.jsonl --prefix=/registry/events/kyuubi \
    --strip-suffix=. --group-depth=4 --sort=count --top=10
```

> 📌 **生产推荐流程：** 大集群优先 `look --keys-only --write-out=jsonl --output=keys.jsonl` 在线导出一次，再用 `summary --input=keys.jsonl` 离线多维分析，避免每次 summary 都重新扫描 etcd。在线 summary 定位为小集群/小前缀/临时确认的便捷模式。详见第八章。

### 7.7 `unmarshal` — Proto 反序列化

**最实用的特色功能**，直接用 `.proto` 源文件反序列化 etcd 中的 protobuf 数据，无需编译 Go 代码。

```bash
etcdctl+ unmarshal \
  --target-key by-dev/meta/channelwatch/4/by-dev-rootcoord-dml_0_445337303926193462v0 \
  --import-path /path/to/proto/dir \
  --proto /path/to/proto/dir/data_coord.proto \
  --full-message-name milvus.protov2.data.ChannelWatchInfo
```

| 参数 | 说明 |
|------|------|
| `--target-key` | etcd 中的完整 key |
| `--import-path` | proto 文件的 import 搜索目录（可指定多个） |
| `--proto` | 目标 message 所在的 `.proto` 文件路径（可指定多个） |
| `--full-message-name` | 完整 message 名称（`包名.消息名`） |

**输出示例：**

```
vchan: collectionID:445337303926193462 channelName:"by-dev-rootcoord-dml_0"
state: 3
startTs: 1698912217
progress: 0
```

**K8s 场景下的使用：**

Kubernetes 在 etcd 中存储的也是 protobuf 编码数据，可以用这个命令解码：

```bash
# 解码 K8s Pod 数据（需要 k8s 的 proto 文件）
etcdctl+ unmarshal \
  --target-key /registry/pods/default/my-pod \
  --import-path /path/to/kubernetes/staging/src/k8s.io/api/core/v1 \
  --proto /path/to/kubernetes/staging/src/k8s.io/api/core/v1/generated.proto \
  --full-message-name k8s.io.api.core.v1.Pod
```

> ⚠️ **Breaking Change：** 修复前版本使用 `--key`，与全局 TLS `--key` 冲突，TLS 环境下会导致连接失败。
> - 修复前：`etcdctl+ unmarshal --key=/registry/pods/default/my-pod`（TLS 环境下有 bug）
> - 修复后：`etcdctl+ unmarshal --target-key=/registry/pods/default/my-pod`

### 7.8 `leader` — 查询 leader 节点

```bash
etcdctl+ leader
```

输出：

```
Name: default
ClientUrls: [http://127.0.0.1:2379]
```

**用途：** 快速确认集群当前 leader，排查脑裂或切换问题。

### 7.9 `decode` — Base64 解码

```bash
# look 命令 --show-value 输出的值是 base64 编码的，用 decode 解码
etcdctl+ decode --value="aGVsbG8gd29ybGQ="
```

输出：

```
decode value:
 hello world
```

**注意：** 这个命令不需要连接 etcd，纯本地操作。

### 7.10 `rename` — 重命名 key

```bash
# 重命名（默认备份旧 key 到 etcd-bak/ 前缀下）
etcdctl+ rename --source-key=/old/key --target-key=/new/key

# 重命名但不备份
etcdctl+ rename --source-key=/old/key --target-key=/new/key --bak=false
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--source-key` | 空 | 原 key |
| `--target-key` | 空 | 新 key |
| `--bak` | `true` | 是否备份旧 key-value 到 `etcd-bak/` 前缀 |

> ⚠️ **注意：** 此操作非原子（Get→Put→Delete），中途失败可能产生不一致。建议始终开启 `--bak`。

### 7.11 `clear` — 清空所有数据

```bash
etcdctl+ clear
```

输出：

```
Current Data Count: 399
Clear All Data, (Y/n): Y   # 必须输入大写 Y 确认
```

> ⚠️ **危险操作！** 会删除 etcd 中所有数据，不可逆。

---

## 八、典型使用场景

### 8.1 场景覆盖关系

| 排查目标 | 影响 | 核心命令 | 修复建议 |
|---|---|---|---|
| DB size 增长 | 接近 quota 触发 NOSPACE；snapshot/restore/defrag 时间变长 | `summary --sort=count/total-size` | Prometheus 区分真实增长 vs 碎片：碎片增长 → 低峰期 compact + 逐节点 defrag；真实增长 → 定位大头前缀后推动业务清理（如过期 events、废弃 CRD） |
| revision rate 高 | WAL/fsync、apply、watch event 压力升高 | `summary --sort=max-version`、`--min-mod-revision` | 定位热点前缀后推动业务降低写频率；心跳/状态上报类改 Lease KeepAlive 替代高频 Put；无意义轮询写改为 watch + 按需更新 |
| 大量 watcher event | 慢 watcher、pending event、客户端消费延迟 | `summary --sort=latest-mod-revision/max-version` | 先治理写入源（同 revision rate）；消费慢则排查客户端处理逻辑、拆分 watch 范围、加消费并发；客户端来源需结合 apiserver audit |
| 高频覆盖写 | DB in-use 不一定增长，但 revision/WAL/watch 持续增长 | `summary --sort=max-version` | 业务治理：降低更新频率、合并写入、状态心跳改 Lease；确认是否真需要每次写（如 leader election 可拉长周期）；必要时迁移到更适合高频写的存储 |
| 大量新增 key | DB in-use 同步增长，可能由事件、任务、临时对象堆积导致 | `summary --min-create-revision --sort=count` | 业务治理：确认泄漏源（如 events 未配 TTL、Job/Pod 创建后不清理）；加 TTL / ownerReference 自动清理；控制创建速率；必要时调整 `--event-ttl` 或清理策略 |
| 大 value | DB size、网络传输、watch 内存和反序列化开销升高 | `distribute --type=value` + `summary --sort=max-size` | 业务治理：缩小 value（如清理冗余 annotation/managedFields）；拆分大对象到 ConfigMap/Secret 外部存储；避免在 etcd value 中嵌入日志/二进制内容 |
| 历史 revision 堆积（含 tombstone） | compaction 前历史 revision 占用 DB 空间；删除重建型热点 version=1 看不出来 | `look --snapshot` + `summary --sort=rev-count/tombstone-count` | 离线 snapshot db 分析：`rev-count` 定位高 revision 数的 key；`tombstone-count` 定位频繁删除的 key |
| WAL 写入热点 | WAL 持续增长、fsync 延迟升高 | `wal-look` + `wal-summary --sort=put-count` | 定位 Put 次数最多的 key 后推动业务降低写频率；区分 Put/Delete/Txn 操作类型，针对性优化 |
| WAL 操作类型分布 | 需要了解写入模式：以 Put 为主还是 Delete/Txn 为主 | `wal-summary --sort=put-count/delete-count` | 大量 Delete → 检查是否有批量清理任务冲击；大量 Txn → 检查事务是否可简化 |

### 8.2 通用排查流程

排查分三种数据源，按需选择：

**在线分析**（生产大集群三步：Prometheus → keys-only 快照 → 离线 summary）：

```bash
# 第一步：导出 keys-only 快照（在线扫一次，不拉 value）
etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl \
  --endpoints=<follower> --page-sleep=50ms

# 第二步：离线多维分析（不再访问 etcd，可反复跑）
etcdctl+ summary --input=keys.jsonl --group-depth=2 --sort=count --top=50
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=max-version --top=50
etcdctl+ summary --input=keys.jsonl --min-mod-revision=<rev> --sort=count --top=50

# 第三步：对可疑前缀再拉 value size（按需）
etcdctl+ look --prefix=/registry/events --write-out=jsonl --output=events.jsonl
etcdctl+ summary --input=events.jsonl --sort=total-size --top=50
```

**离线 snapshot db 分析**（完全不连集群，一次导出全部字段）：

```bash
# 导出 snapshot → 一次遍历输出全部字段（含 value size + rev_count + tombstone_count）
etcdctl snapshot save cluster.db
etcdctl+ look --snapshot=cluster.db --write-out=jsonl --output=snapshot.jsonl

# 多次分析（不再访问 db）
etcdctl+ summary --input=snapshot.jsonl --group-depth=2 --sort=count --top=50
etcdctl+ summary --input=snapshot.jsonl --sort=rev-count --top=50
etcdctl+ distribute --input=snapshot.jsonl --type=kv
```

**离线 WAL 分析**（分析写操作流：Put/Delete/Txn 类型和频次）：

```bash
# 导出 WAL → 离线分析
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=jsonl --output=wal.jsonl
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --top=50
```

小集群/测试环境可以跳过导出，直接在线 summary：

```bash
etcdctl+ summary --keys-only --group-depth=2 --sort=count --top=50
```

> **keys-only 说明**：`--keys-only` 仅用于**在线分析**——etcd Range API 的 `WithKeysOnly()` 让 server 端跳过 value 内容传输，减少网络开销。**离线 `look --snapshot` 不需要 `--keys-only`**：bbolt 遍历必须 `proto.Unmarshal` 才能拿到 key 名，Unmarshal 后 `len(kv.Value)` 几乎零成本，因此离线默认输出全部字段（含 value size + rev_count + tombstone_count），一次导出即可覆盖所有分析维度。

> **三种数据源的互补关系**：在线分析看当前态（version/mod_revision 等存量指标）；snapshot db 离线分析能看到在线看不到的历史 revision 数和 tombstone（compaction 前的全部修改历史）；WAL 分析看最近的写操作流（Put/Delete/Txn 类型、频次、按 key 聚合），三者互补。

### 8.3 生产安全原则

1. **优先 follower**：只传单个 follower endpoint
2. **低峰期执行**
3. **先 keys-only 后 value**：先用 keys-only 找到可疑前缀，再对可疑前缀拉 value metadata
4. **避免 `look --hang`**：不要持续全量扫描
5. **离线优先**：200 集群批量巡检场景用 `etcdctl snapshot save` 拷副本 → 本地 `look --snapshot` 分析，完全不影响集群

---

### 场景 1：排查大 value

```text
问题：etcd DB 变大、watch 延迟、客户端读取慢，怀疑有大 value。
```

**在线分析**：

```bash
# 看 value 大小分布
etcdctl+ distribute --type=value --bucket=10

# 定位大 value 所在 key
etcdctl+ look --filter=value --filter-min=1048576 --write-out=log

# 对可疑前缀聚合 value size
etcdctl+ look --prefix=/registry/configmaps --write-out=jsonl --output=configmaps.jsonl
etcdctl+ summary --input=configmaps.jsonl --group-depth=3 --sort=max-size --top=50

# 解码大 value 内容
etcdctl+ decode --value=”<base64 value>”
etcdctl+ unmarshal --target-key=/problematic/key --import-path=... --proto=... --full-message-name=...
```

**离线 snapshot db 分析**：

```bash
etcdctl+ look --snapshot=cluster.db --write-out=jsonl --output=cluster.jsonl
etcdctl+ summary --input=cluster.jsonl --group-depth=3 --sort=max-size --top=50
etcdctl+ distribute --input=cluster.jsonl --type=value
```

> `look --filter=value` 是客户端过滤，底层仍会拉 value。生产环境建议先用 `distribute --prefix=<可疑前缀>` 缩小范围，或用离线 snapshot 分析。

---

### 场景 2：DB size 增长定位

```text
问题：etcd_mvcc_db_total_size_in_bytes 持续增长。
```

先用 Prometheus 区分真实增长 vs 碎片增长：

```promql
etcd_mvcc_db_total_size_in_bytes                    -- db_total
etcd_mvcc_db_total_size_in_use_in_bytes              -- db_in_use
etcd_debugging_mvcc_keys_total or etcd_mvcc_keys_total
```

- `db_total` 和 `db_in_use` 同时增长 → 真实数据增长
- `db_total` 增长但 `db_in_use` 不变 → 碎片，走 compact + defrag
- `keys_total` 同步增长 → 大量新增 key

**在线分析**：

```bash
# keys-only 快照 → 找 key 数大头
etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl
etcdctl+ summary --input=keys.jsonl --group-depth=2 --sort=count --top=50

# key 数不能解释 DB 增长 → 对可疑前缀拉 value size
etcdctl+ look --prefix=/registry/events --write-out=jsonl --output=events.jsonl
etcdctl+ summary --input=events.jsonl --group-depth=3 --sort=total-size --top=50
```

**离线 snapshot db 分析**（一次导出即可看 size + revision 堆积）：

```bash
etcdctl+ look --snapshot=cluster.db --write-out=jsonl --output=snapshot.jsonl
etcdctl+ summary --input=snapshot.jsonl --group-depth=2 --sort=count --top=50

# 历史 revision 排行：找删除重建型热点（在线 version=1 看不出来）
etcdctl+ summary --input=snapshot.jsonl --sort=rev-count --top=50
```

---

### 场景 3：revision rate 高 / 高频覆盖写

```text
问题：rate(current_revision[5m]) 升高，或少量 key 被频繁 PUT（version 很高但 key 数和 DB 不增长）。
```

Prometheus 确认：

```promql
rate(etcd_debugging_mvcc_current_revision[5m])   -- 或 etcd_mvcc_current_revision
rate(etcd_mvcc_put_total[5m])
```

如果能从 Grafana 拿到高峰开始时间对应的 revision（如 `18:30_rev = 38770000000`）：

**在线分析**：

```bash
etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl

# 高 version 热点前缀
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=max-version --top=50

# 指定时间点后修改最多的前缀
etcdctl+ summary --input=keys.jsonl --min-mod-revision=38770000000 --group-depth=3 --sort=count --top=50

# 最近活跃前缀（没有 revision 时的替代）
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=latest-mod-revision --top=50
```

**离线 WAL 分析**（看最近写操作的真实 Put 次数，比 version 更直接）：

```bash
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=jsonl --output=wal.jsonl
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --top=50
```

---

### 场景 4：大量新增 key / 某类资源过多

```text
问题：keys_total 快速增长，或总 key 数很高需要定位大头。
```

**在线分析**：

```bash
etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl

# 按资源类型聚合
etcdctl+ summary --input=keys.jsonl --group-depth=2 --sort=count --top=50

# 按 namespace 下钻
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=count --top=50

# 指定 revision 后新增最多的前缀
etcdctl+ summary --input=keys.jsonl --min-create-revision=<start_rev> --group-depth=3 --sort=count --top=50
```

**离线 snapshot db 分析**（批量巡检 200 集群时用 snapshot 副本，不连集群）：

```bash
etcdctl+ look --snapshot=cluster.db --write-out=jsonl --output=snapshot.jsonl
etcdctl+ summary --input=snapshot.jsonl --group-depth=2 --sort=count --top=50
```

---

### 场景 5：watcher event 升高

```text
问题：watch 消息量、mvcc events 指标升高。
```

Prometheus 判断 watcher 是被写入带起来还是消费慢：

```promql
rate(etcd_debugging_mvcc_events_total[5m])
etcd_debugging_mvcc_slow_watcher_total
```

**在线分析**：

```bash
etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl

# 找最近活跃写入前缀
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=latest-mod-revision --top=50

# 找高频覆盖写前缀
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=max-version --top=50
```

**离线 WAL 分析**（直接看最近一段时间的写操作流）：

```bash
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=jsonl --output=wal.jsonl

# 按 key 聚合 Put 次数，找真正的写入热点
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --top=50
```

> etcd 存量数据只能定位写入前缀，不能直接定位 watcher 客户端 IP。客户端来源需结合 apiserver audit 或业务日志。

---

### 场景 6：历史 revision 堆积 / 删除重建热点

```text
问题：compaction 不及时导致历史 revision 堆积占用 DB；或某些 key 反复删除重建，version 重置为 1 在线看不出来。
```

这是**在线分析的盲区**——在线 Range 只返回当前态，看不到历史 revision 数。必须用离线 snapshot db 分析。

```bash
# 导出 snapshot
etcdctl snapshot save cluster.db

# 一次导出全部字段（含 rev_count + tombstone_count）
etcdctl+ look --snapshot=cluster.db --write-out=jsonl --output=snapshot.jsonl

# 历史 revision 排行：找 revision 数最多的 key
etcdctl+ summary --input=snapshot.jsonl --sort=rev-count --top=50

# tombstone 排行：找被删除次数最多的 key
etcdctl+ summary --input=snapshot.jsonl --sort=tombstone-count --top=50

# 原始数据查看
etcdctl+ dump iterate-bucket --snapshot=cluster.db key --limit=20
```

> **为什么在线看不到**：etcd v3 在线 Range 每个 key 只返回一条最新值，`version` 字段在删除重建后重置为 1。只有离线遍历 bbolt 的 key bucket 才能看到 compaction 前的全部历史 revision，包括 tombstone 条目。

---

### 场景 7：WAL 写入热点分析

```text
问题：WAL 持续增长、fsync 延迟升高，需要定位写入最多的 key。
```

```bash
# 导出 WAL 操作流
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=jsonl --output=wal.jsonl

# 按 key 聚合 Put 次数排行
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --top=50

# 只看某个 index 范围内的操作
etcdctl+ wal-look --data-dir=/var/lib/etcd --start-index=930 --end-index=1000 --write-out=jsonl --output=wal-range.jsonl

# 按操作类型过滤
etcdctl+ wal-look --data-dir=/var/lib/etcd --entry-type=IRRPut,IRRDeleteRange --write-out=jsonl --output=wal-writes.jsonl
etcdctl+ wal-summary --input=wal-writes.jsonl --sort=put-count --top=50

# 原始 WAL 条目查看
etcdctl+ dump wal --data-dir=/var/lib/etcd --entry-type=IRRPut --start-index=930 --end-index=932
```

> **WAL vs 在线 version 的区别**：在线 `version` 是 key 自最近创建以来的修改次数（删除重建后重置），WAL 记录的是 raft 日志中每次实际写操作，不受重置影响。WAL 覆盖最近几个 snapshot 周期的数据，历史范围比 snapshot db 短。

---

### 场景 8：WAL 操作类型分布

```text
问题：需要了解集群写入模式——以 Put 为主还是 Delete/Txn 为主，用于容量规划或异常排查。
```

```bash
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=jsonl --output=wal.jsonl

# Put 热点
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --top=50

# Delete 热点
etcdctl+ wal-summary --input=wal.jsonl --sort=delete-count --top=50

# 快速一步分析（不导出 JSONL，但每次都重新解析 WAL）
etcdctl+ wal-summary --data-dir=/var/lib/etcd --sort=put-count --top=50
```

---

### 场景 9：数据迁移 / 清理前确认

```bash
etcdctl+ leader
etcdctl+ find --match-key=deprecated --prefix=/old-system --limit=100
```

注意：`clear` 和 `rename` 属于高风险写操作，当前文档建议禁用或仅在明确授权和备份后使用。

---

## 九、高风险命令分析

涉及数据写操作或可能对集群产生较大压力的命令有 **3 个**：

| 风险等级 | 命令 | 操作类型 | 风险说明 |
|---------|------|---------|---------|
| 🔴 极高 | **clear** | `Delete(WithFromKey)` | 删除 etcd **全部**数据，不可逆，即使有确认提示也无法回滚 |
| 🟠 高 | **rename** | `Get→Put→Delete` | 非原子三步操作，中途失败会导致旧 key 已删、新 key 未写或重复写入 |
| 🟡 中 | **look**（hang 模式） | 全量循环读 | `--hang=true` 时每间隔全量读取一次，持续对集群施加大读压力 |

**详细风险分析：**

### 9.1 🔴 clear — 极高风险

```go
// core/data_source.go + cmd/clear_cmd.go
// 核心操作：
core.EtcdDelete(client, core.EmptyChar(), clientv3.WithFromKey())
// EmptyChar() 返回 \x00，WithFromKey() 表示从该 key 开始删除所有 key
// = 删除 etcd 中全部数据
```

- 虽然有 `Y/n` 确认，但一旦确认后**不可回滚**
- 无 prefix 选项，无法限定范围，只能全删
- 生产环境误操作后果严重

### 9.2 🟠 rename — 高风险

```go
// cmd/rename_cmd.go 核心操作序列：
resp, _ := core.EtcdGet(client, renameSourceKey)        // 1. 读取旧值
core.EtcdPut(client, "etcd-bak/"+renameSourceKey, ...)  // 2. 备份（可选）
core.EtcdPut(client, renameTargetKey, ...)              // 3. 写入新 key
core.EtcdDelete(client, renameSourceKey)                 // 4. 删除旧 key
```

- 三步操作（Get→Put→Delete）**不是事务**，不具备原子性
- 第 3 步 Put 成功但第 4 步 Delete 失败 → 旧 key 和新 key 同时存在
- 第 2 步备份失败但第 3/4 步成功 → 旧 key 被删且无备份
- 无 rollback 机制

### 9.3 🟡 look --hang — 中风险

```go
// cmd/look_cmd.go hang 模式：
ct := time.Tick(time.Second * time.Duration(hangInterval))
for {
    select {
    case <-ct:
        resp, datac = core.GetAllData()  // 每次全量读取
        appendBuffer(resp, datac, writer)
    }
}
```

- 每次 tick 都全量扫描 etcd，`--hang-interval` 默认仅 2 秒
- 大数据量集群下持续产生高读压力
- 与 distribute 类似，但 distribute 只读一次，hang 模式会循环读取

### 9.4 安全命令（只读，无风险）

| 命令 | 操作类型 | 说明 |
|------|---------|------|
| **distribute** | 只读统计 | 全量读一次，统计后退出 |
| **find** | 只读搜索 | `strings.Contains` 匹配，不修改数据 |
| **leader** | 只读查询 | `MemberList` + `Status`，仅查元数据 |
| **decode** | 纯本地操作 | Base64 解码，不连接 etcd |
| **unmarshal** | 只读 Get + 本地反序列化 | 仅 Get 一个 key，不修改数据 |

### 9.5 高危命令禁用

已禁用 `clear` 和 `rename` 两个高危命令，防止误操作导致 etcd 数据丢失或不一致。

**修改文件：** `cmd/root_cmd.go`

```go
rootCmd.AddCommand(NewLeaderCmd())
// Disabled: high-risk commands that modify/delete etcd data
// rootCmd.AddCommand(NewClearCmd())   // 🔴 deletes ALL etcd data, irreversible
rootCmd.AddCommand(NewFindCmd())
rootCmd.AddCommand(NewDecodeCmd())
// rootCmd.AddCommand(NewRenameCmd())  // 🟠 non-atomic Get→Put→Delete, may cause inconsistency
rootCmd.AddCommand(NewUnmarshalCmd())
```

**代码扫描确认无隐藏调用：**

- `EtcdPut` 和 `EtcdDelete` 仅在 `clear_cmd.go` 和 `rename_cmd.go` 中被调用，其他命令未使用
- 所有命令注册集中在 `root_cmd.go` 的 `init()` 函数中，没有 `init()` 自注册、反射加载或动态发现机制
- 注释掉 `AddCommand` 后，两个命令完全不可达

**禁用后效果验证：**

```bash
# 帮助信息中不再显示 clear 和 rename
$ etcdctl+ --help
Available Commands:
  completion        Generate the autocompletion script for the specified shell
  decode            decode the base64-encoded value
  distribute        Show the data distribution of etcd
  find              find the key from all etcd data
  help              Help about any command
  leader            Get the leader node info
  look              Look all etcd data
  unmarshal         unmarshal the etcd value

# 直接调用会报未知命令
$ etcdctl+ clear
Error: unknown command "clear" for "etcdctl+"

$ etcdctl+ rename
Error: unknown command "rename" for "etcdctl+"
```

**如需重新启用：** 取消 `root_cmd.go` 中对应行的注释，重新编译即可。`clear_cmd.go` 和 `rename_cmd.go` 源码已保留。

---

## 十、`--bucket` 直方图桶数详解

### 10.1 什么是桶（bucket）

`--bucket` 参数控制直方图的**区间数量**。把 [Smallest, Largest] 的范围等分为 N 个区间，统计每个区间内的数据条数。

**区间宽度计算：** `(Largest - Smallest) / bucket`

### 10.2 示例对比

假设 etcd 中有 100 条数据，value 大小范围是 0B ~ 1000B：

**`--bucket=5`（默认）：** 把 [0, 1000] 等分为 5 个区间，每段 200B

```
Size histogram:
  0 B    [20]   |∎∎∎∎∎∎∎∎∎∎
  200 B  [35]   |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
  400 B  [25]   |∎∎∎∎∎∎∎∎∎∎∎∎∎
  600 B  [10]   |∎∎∎∎∎
  800 B  [10]   |∎∎∎∎∎
  1000 B [0]    |
```

**`--bucket=8`：** 等分为 8 个区间，每段 125B，粒度更细

```
Size histogram:
  0 B    [12]   |∎∎∎∎∎∎
  125 B  [8]    |∎∎∎∎
  250 B  [20]   |∎∎∎∎∎∎∎∎∎∎
  375 B  [15]   |∎∎∎∎∎∎∎∎
  500 B  [15]   |∎∎∎∎∎∎∎∎
  625 B  [12]   |∎∎∎∎∎∎
  750 B  [8]    |∎∎∎∎
  875 B  [10]   |∎∎∎∎∎
  1000 B [0]    |
```

### 10.3 bucket 值选择建议

| bucket 值 | 区间宽度 | 粒度 | 适用场景 |
|-----------|---------|------|---------|
| 3~5 | 宽 | 粗略概览 | 快速看分布趋势 |
| 8~10 | 适中 | 适中 | 精确定位大 value 所在区间 |
| 15~20 | 窄 | 细致 | 数据量大、需要精细分析 |

---

## 十一、注意事项

1. **无密码认证**：不支持 etcd 的 `--auth` 模式，只能连无认证或 TLS 认证的集群
2. **distribute 读全量数据**：会对集群产生读压力，生产环境建议在非高峰期使用
3. **clear 不可逆**：删除前会显示数据条数并要求确认，但操作不可回滚（详见第九章）
4. **rename 非原子**：三步操作不是事务，失败可能产生中间状态（详见第九章）
5. **look 的 `--hang` 模式**：每次刷新都是全量读取，长时间运行会产生持续读压力（详见第九章）
6. **先用 distribute 评估数据量，再决定是否跑 look**：distribute 和 look 都会全量读取 etcd 数据，distribute 只读一次且输出统计摘要，look 会返回所有 KV 数据。建议先用 `distribute --type=kv` 查看数据总量和条数，数据量大时（几十万条+）谨慎使用 look，避免对生产集群造成过大读压力

---
