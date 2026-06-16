# etcd-analysis 项目分析

## 一、项目定位

etcd-analysis 是一个 **etcd 数据分析 CLI 工具**（二进制名 `etcdctl+`），核心解决的问题是：**etcd 中存储的数据大小分布不可见**。etcd 对 key-value 大小敏感，大 value 会影响 watch 稳定性和内存占用，这个工具让运维/测试人员能直观地看到 etcd 中数据的大小分布、快速检索和解码数据。

> etcd 通常用于存储系统元数据或服务发现，适合存储小型键值对。同时 etcd 对键值对的大小很敏感，存储大键值对时，如果数量过多，会带来很多不良影响，例如 watch 功能的稳定性降低，以及占用大量内存。

项目地址：[https://github.com/SimFG/etcd-analysis](https://github.com/SimFG/etcd-analysis)

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
│      ├── find_cmd.go         按关键字搜索 key        │
│      ├── leader_cmd.go       查询 leader 节点信息    │
│      ├── clear_cmd.go        清空所有数据            │
│      ├── decode_cmd.go       Base64 解码            │
│      ├── rename_cmd.go       重命名 key              │
│      └── unmarshal_cmd.go    Proto 反序列化          │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                  core/ (核心层)                      │
│  config.go      ─── 全局配置 Cfg (endpoints/TLS)     │
│  data_source.go ─── etcd 客户端初始化 + 流式数据读取  │
│  etcd_op.go     ─── etcd CRUD 操作封装 (带超时)      │
│  report.go      ─── 统计报表引擎 (直方图/百分位)      │
│  percent.go     ─── 百分位数计算 (P10~P99)           │
│  util.go        ─── 通用工具函数                     │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│            etcd client/v3 (官方 SDK)                │
│         protoreflect (proto 动态解析)               │
│            uilive (终端实时刷新输出)                  │
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

## 四、8 个子命令原理

| 命令 | 功能 | 核心原理 |
|------|------|---------|
| **distribute** | 数据大小分布分析 | 流式读取 → Report 统计 → 直方图 + 百分位实时渲染 |
| **look** | 查看/导出全量数据 | 流式读取 → 表格输出 (stdout/file/log)，支持 filter 按大小过滤，hang 模式定时刷新 |
| **find** | 按关键字搜索 key | 流式读取 → `strings.Contains` 匹配，支持 prefix + limit |
| **leader** | 查询 leader 节点 | `MemberList` + `Status` 获取 leader ID → 匹配 member 信息 |
| **clear** | 清空所有数据 | 先 `WithCountOnly` 统计数量 → 确认 → `Delete(WithFromKey)` 全删 |
| **decode** | Base64 解码 | 纯本地 `base64.StdEncoding.DecodeString`，不需要连接 etcd |
| **rename** | 重命名 key | Get 旧值 → Put 新 key → 可选备份到 `etcd-bak/` 前缀 → Delete 旧 key |
| **unmarshal** | Proto 反序列化 | Get 原始字节 → `protoparse` 解析 .proto 源文件 → `dynamic.Message` 动态反序列化 → 打印字段 |

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
       ├── core.InitClient()  ──→  建立 etcd v3 client 连接 (随机选 endpoint)
       │                            └── EtcdStatus() 健康检查
       │
       ├── core.GetAllData() / GetDataWithPrefix()  ──→  流式分页读取
       │       │
       │       └── chan []*mvccpb.KeyValue  (生产者-消费者模式)
       │
       └── 业务逻辑处理
              │
              ├── distribute: 喂入 Report → 统计 → 直方图/百分位
              ├── look:       过滤 → 格式化表格 → 写 stdout/file/log
              ├── find:       strings.Contains 匹配 → 输出
              ├── unmarshal:  protoreflect 动态解析 → 打印结构化字段
              └── 其他:       直接调用 etcd_op 封装
```

---

## 六、设计亮点与不足

**亮点：**

1. **流式分页读取**：全量读取时不一次性加载到内存，用 channel 做生产者-消费者解耦，适合大数据量场景
2. **Serializable 读**：`WithSerializable()` 降低读请求对 Raft 提议管道的压力，减少对在线业务的影响
3. **实时终端渲染**：利用 `uilive` 库在 distribute 命令中每 100ms 刷新输出，用户体验好
4. **Proto 动态反序列化**：unmarshal 命令不需要编译目标 proto，直接用源文件解析，实用性很强
5. **随机 endpoint**：分散读压力，避免单节点过载

**不足：**

1. **无认证支持**：不支持 etcd 的 user/password 认证，只能连无认证或 TLS 认证的集群
2. **全局变量较多**：`core` 包大量使用包级全局变量（`client`、`C`），不利于测试和并发
3. **rename 非原子**：rename 操作是 Get→Put→Delete 三步，非事务操作，中途失败会留下不一致状态
4. **clear 无 prefix 选项**：只能清空全部数据，无法按前缀清理
5. **错误处理粗暴**：大量 `core.Exit(err)` 直接 `os.Exit(-1)`，没有优雅的资源清理

---

## 七、使用指南

### 7.1 安装

```bash
# 克隆仓库
git clone https://github.com/SimFG/etcd-analysis.git
cd etcd-analysis

# 编译
go build -o etcdctl+
```

编译后得到可执行文件 `etcdctl+`。

### 7.2 全局参数

所有子命令共享以下参数（由 `root_cmd.go` 定义）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--endpoints` | `127.0.0.1:2379` | etcd 集群地址，多个用逗号分隔 |
| `--cert` | 空 | TLS 客户端证书文件 |
| `--key` | 空 | TLS 客户端私钥文件 |
| `--cacert` | 空 | TLS CA 证书文件 |
| `--command-timeout` | `5` | 操作超时时间（秒） |

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
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--type` | `key` | 统计维度：`key` / `value` / `kv` |
| `--bucket` | `5` | 直方图桶数 |

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

# 只看 value 大小在 74~100 字节之间的数据
etcdctl+ look --filter=key --filter-min=74 --filter-max=100

# 输出为日志格式（适合 loki 等日志系统采集）,"log" 和 "file" 均是文件写入
etcdctl+ look --write-out=log

# 配合管道使用
etcdctl+ look | more
etcdctl+ look | vim -
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--show-value` | `false` | 是否显示 value（base64 编码） |
| `--write-out` | `stdout` | 输出方式：`stdout` / `file` / `log` |
| `--hang` | `false` | 持续刷新（需配合 `--write-out=file`） |
| `--hang-interval` | `2` | 刷新间隔（秒） |
| `--filter` | `none` | 大小过滤维度：`none` / `key` / `value` / `kv` |
| `--filter-min` | `-1` | 最小值（字节） |
| `--filter-max` | `-1` | 最大值（字节） |

**输出示例（stdout 模式）：**

```
Current Stage
  cluster_id:2037210783374497686 member_id:13195394291058371180 revision:254946 raft_term:9
Kv List
| Key | Value | Size | CreateRevision | ModRevision | Version | Lease |
| /config/server | - | 85.0 B | 1 | 1 | 1 | 0 |
```

**输出示例（log 模式）：**

```
key=/config/server value=- size=85.0 B create_revision=1 mod_revision=1 version=1 lease=0
```

⚠️ 性能提示： look 会全量读取 etcd 所有数据，数据量大时对集群读压力较大。建议先用 distribute --type=kv 查看数据总量和条数，数据量大时（几十万条+）谨慎使用 look，避免对生产集群造成过大读压力。

### 7.5 `find` — 按关键字搜索 key

```bash
# 按前缀搜索
etcdctl+ find --prefix=/registry/pods

# 搜索并显示 value
etcdctl+ find --match-key=index --value

# 限制返回数量
etcdctl+ find --match-key=index --limit=50
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--match-key` | 空 | 搜索关键字（包含匹配） |
| `--prefix` | 空 | key 前缀过滤 |
| `--value` | `false` | 是否显示 value |
| `--limit` | `10` | 最大返回数量 |

> ⚠️ **Breaking Change：** 修复前版本使用 `--key`，与全局 TLS `--key` 冲突，TLS 环境下会导致连接失败。
> - 修复前：`etcdctl+ find --key=index`（TLS 环境下有 bug）
> - 修复后：`etcdctl+ find --match-key=index`

### 7.6 `unmarshal` — Proto 反序列化

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

### 7.7 `leader` — 查询 leader 节点

```bash
etcdctl+ leader
```

输出：

```
Name: default
ClientUrls: [http://127.0.0.1:2379]
```

**用途：** 快速确认集群当前 leader，排查脑裂或切换问题。

### 7.8 `decode` — Base64 解码

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

### 7.9 `rename` — 重命名 key

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

### 7.10 `clear` — 清空所有数据

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

### 场景 1：排查 etcd 性能问题 — 发现大 value

```bash
# 第一步：看 value 大小分布
etcdctl+ distribute --type=value --bucket=10

# 发现大量 value > 1MB → 定位具体是哪些 key
etcdctl+ look --filter=value --filter-min=1048576 --show-value

# 解码大 value 的内容（如果是 protobuf）
etcdctl+ decode --value="<base64 value>"
# 或（修复后版本）
etcdctl+ unmarshal --target-key=/problematic/key --import-path=... --proto=... --full-message-name=...
# 修复前版本：etcdctl+ unmarshal --key=/problematic/key --import-path=... --proto=... --full-message-name=...
```

### 场景 2：运维巡检 — 监控 etcd 数据增长

```bash
# 持续导出全量数据到文件，配合 vim/tail 观察
etcdctl+ look --write-out=file --hang=true --hang-interval=30

# 另一个终端
tail -f analysis.txt
```

### 场景 3：数据迁移/清理

```bash
# 查看集群 leader
etcdctl+ leader

# 搜索待清理的 key（修复后版本）
etcdctl+ find --match-key=deprecated --prefix=/old-system --limit=100
# 修复前版本：etcdctl+ find --key=deprecated --prefix=/old-system --limit=100

# 重命名迁移
etcdctl+ rename --source-key=/old/key --target-key=/new/key

# 确认无误后清理（谨慎！）
etcdctl+ clear
```

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

## 十一、etcd client v3.5.0 `WithPrefix` 误判 Bug 分析

### 11.1 Bug 现象

执行 `distribute` 命令时触发 panic：

```
panic: `WithPrefix` and `WithFromKey` cannot be set at the same time, choose one
```

### 11.2 根因

etcd client v3.5.0 用**反射 + 字符串包含**判断是否传入了某个 option：

```go
// etcd client v3.5.0 utils.go
func isOpFuncCalled(op string, opts []OpOption) bool {
    for _, opt := range opts {
        v := reflect.ValueOf(opt)           // 拿到闭包的反射值
        if v.Kind() == reflect.Func {
            if opFunc := runtime.FuncForPC(v.Pointer()); opFunc != nil {
                if strings.Contains(opFunc.Name(), op) {  // 用 Contains 匹配函数名
                    return true
                }
            }
        }
    }
    return false
}
```

Go 运行时给闭包命名时，**会带上外层函数名**。当在 `GetDataWithPrefix` 函数内调用 `WithFromKey()` 时，闭包的实际名称为：

```
main.GetDataWithPrefix.WithFromKey.func3
      ^^^^^^^^^^^^^^^
      外层函数名包含 "WithPrefix"
```

因此检测逻辑发生误判：

```
IsOptsWithPrefix(opts)?
  → 遍历所有 option，检查函数名是否包含 "WithPrefix"
  → WithFromKey 闭包名 = "main.GetDataWithPrefix.WithFromKey.func3"
  → strings.Contains("...GetDataWithPrefix.WithFromKey...", "WithPrefix") = true ❌ 误判！

IsOptsWithFromKey(opts)?
  → strings.Contains("...GetDataWithPrefix.WithFromKey...", "WithFromKey") = true ✅ 正确

两个都 true → panic！
```

**同理，`WithSerializable` 和 `WithLimit` 的闭包名也被误判为 "WithPrefix"：**

```
main.GetDataWithPrefix.WithSerializable.func1  → Contains("WithPrefix") = true ❌
main.GetDataWithPrefix.WithLimit.func2         → Contains("WithPrefix") = true ❌
main.GetDataWithPrefix.WithFromKey.func3       → Contains("WithPrefix") = true ❌
```

### 11.3 触发链路

```
用户执行: etcdctl+ distribute
    │
    ▼
cmd/distribute_cmd.go
    core.GetAllData()  →  core.GetDataWithPrefix("")    ← prefix 为空
    │
    ▼
core/data_source.go  GetDataWithPrefix(prefix)
    if prefix == "":
        opts = append(opts, clientv3.WithFromKey())     ← 加 WithFromKey
    resp, err := EtcdGet(client, start, opts...)         ← 触发 panic
    │
    ▼
etcd client v3.5.0  OpGet(key, opts...)
    IsOptsWithPrefix(opts) && IsOptsWithFromKey(opts)    ← 误判！
    → panic
```

### 11.4 触发条件

| 条件 | 说明 |
|------|------|
| **etcd client 版本** | ≤ v3.5.0（v3.5.15 确认已修复） |
| **项目代码** | 在函数名包含 `WithPrefix` 的函数内调用 `WithFromKey()` |
| **具体命令** | `distribute`、`look`、`find`（无 prefix 时）都会走 `GetAllData()` → `GetDataWithPrefix("")` |
| **代码路径** | `prefix == ""` → 走 `WithFromKey()` 分支，才会同时被误判为 `WithPrefix` |
| **带 prefix 不会触发** | `find --prefix=xxx` 走 `WithPrefix()` 分支，没有 `WithFromKey`，不会冲突 |

### 11.5 v3.5.15+ 的修复方式

不再依赖反射和字符串匹配，改为在闭包内部直接设置 bool 标记：

```go
// v3.5.15 op.go
type Op struct {
    ...
    isOptsWithPrefix  bool   // 新增字段
    isOptsWithFromKey bool   // 新增字段
}

func WithPrefix() OpOption {
    return func(op *Op) {
        op.isOptsWithPrefix = true    // 直接标记，不再依赖函数名
        ...
    }
}

func WithFromKey() OpOption {
    return func(op *Op) {
        op.isOptsWithFromKey = true   // 直接标记
        ...
    }
}
```

### 11.6 修复方案

升级 etcd client 到 v3.5.27（已验证编译通过、运行正常）：

```
etcd client:  v3.5.0 → v3.5.27
go directive:  1.18  → 1.24
grpc:         v1.57 → v1.71
protobuf:     v1.31 → v1.36
```

不需要修改项目代码，纯依赖升级即可。etcd 客户端版本不需要和服务端版本完全一致，v3.5.27 的客户端可以连 v3.4/v3.5/v3.6 的服务端。

---

## 十二、注意事项

1. **无密码认证**：不支持 etcd 的 `--auth` 模式，只能连无认证或 TLS 认证的集群
2. **distribute 读全量数据**：会对集群产生读压力，生产环境建议在非高峰期使用
3. **clear 不可逆**：删除前会显示数据条数并要求确认，但操作不可回滚（详见第九章）
4. **rename 非原子**：三步操作不是事务，失败可能产生中间状态（详见第九章）
5. **look 的 `--hang` 模式**：每次刷新都是全量读取，长时间运行会产生持续读压力（详见第九章）
6. **先用 distribute 评估数据量，再决定是否跑 look**：distribute 和 look 都会全量读取 etcd 数据，distribute 只读一次且输出统计摘要，look 会返回所有 KV 数据。建议先用 `distribute --type=kv` 查看数据总量和条数，数据量大时（几十万条+）谨慎使用 look，避免对生产集群造成过大读压力

---

## 十三、`--key` Flag 冲突 Bug 分析

### 13.1 Bug 现象

使用 TLS 连接 etcd 时，`find` 和 `unmarshal` 命令报 TLS 握手失败：

```bash
./etcdctl+ \
  --endpoints=https://endpoints.etcd.qa.17usoft.com:5211 \
  --cert=client.pem \
  --key=client-key.pem \
  --cacert=ca.pem \
  find --key=qa
```

报错：

```
tls: failed to verify certificate: x509: "etcd" certificate is not standards compliant
Error: unvaliable etcd server, error: context deadline exceeded
```

而 `leader`、`distribute`、`look` 等命令同样使用 TLS 连接却完全正常。

### 13.2 根因

**全局 flag 和子命令 flag 同名冲突，导致 TLS 私钥路径被覆盖。**

```go
// cmd/root_cmd.go:31 — 全局 PersistentFlag
rootCmd.PersistentFlags().StringVar(&core.C.TLS.KeyFile, "key", "", "identify secure client using this TLS key file")

// cmd/find_cmd.go:31 — find 子命令 LocalFlag
cmd.Flags().StringVar(&findKey, "key", "", "Show the data like the key")

// cmd/unmarsha_cmd.go:29 — unmarshal 子命令 LocalFlag
cmd.Flags().StringVar(&unmarshallKey, "key", "", "the proto.marshal value of the full key")
```

Cobra 的行为：**子命令的 LocalFlag 同名时会覆盖 PersistentFlag**。

当执行：

```bash
./etcdctl+ --key=client-key.pem find --key=qa
```

`--key=qa` 把全局 TLS `--key` 从 `client-key.pem` 覆盖成了 `"qa"`，导致客户端证书加载失败，TLS 握手直接报错。

### 13.3 触发条件

必须**同时满足**两个条件：

| 条件 | 说明 |
|------|------|
| 1. 使用 TLS 连接 | 传了全局 `--key`（TLS 私钥），flag 才有值可被覆盖 |
| 2. 子命令使用了 `--key` | `find --key=xxx` 或 `unmarshal --key=xxx` 覆盖全局值 |

```bash
# 不连 TLS → 全局 --key 为空，find --key=qa 覆盖空值，无影响
etcdctl+ find --key=qa                                    # ✅ 正常

# 连 TLS 但子命令不用 --key → 全局 --key 不被覆盖
etcdctl+ --key=client-key.pem find                        # ✅ 正常

# 连 TLS + 子命令用 --key → 💥 全局 --key 被覆盖
etcdctl+ --key=client-key.pem find --key=qa               # ❌ TLS 握手失败
```

作者大概率只在无 TLS 的本地环境测试，所以从未发现此 bug。

### 13.4 项目中 key 相关 flag 命名格式全览

| 命令 | flag 名 | 语义 | "key" 的角色 | 格式 |
|------|---------|------|-------------|------|
| **root (全局)** | `--key` | TLS 私钥 | — | 单词 |
| distribute | `--type=key` | 按 key 大小统计 | key 是**维度/类别** | `--<维度>=key` |
| look | `--filter=key` | 按 key 大小过滤 | key 是**维度/类别** | `--<维度>=key` |
| find | `--key=qa` ⚠️ | 模糊匹配搜索 key | key 是**目标** | 单词（冲突） |
| unmarshal | `--key=/path/to/key` ⚠️ | 指定完整 key 查询 | key 是**目标** | 单词（冲突） |
| rename | `--source-key=xxx` | 源 key | key 是**目标** | `--<修饰>-key` |
| rename | `--target-key=xxx` | 目标 key | key 是**目标** | `--<修饰>-key` |

### 13.5 命名规律

- key 作为**维度/类别**时 → `--<维度>=key`（如 `--type=key`、`--filter=key`）
- key 作为**目标对象**时 → `--<修饰>-key=<值>`（如 `--source-key`、`--target-key`）

find 和 unmarshal 的 `--key` 都是"目标对象"，应按第二种格式。

`--type=key` 和 `--filter=key` 不合适的原因：

- `--type=key` 在 distribute 里是"统计维度"，find 的语义不是维度
- `--filter=key` 在 look 里是"过滤属性"，unmarshal 的语义不是过滤条件

### 13.6 修复建议

| 命令 | 原 flag | 建议 flag | 理由 |
|------|---------|----------|------|
| find | `--key` | `--match-key` | 模糊匹配目标 key，和 `--source-key`/`--target-key` 风格一致 |
| unmarshal | `--key` | `--target-key` | 精确查询目标 key，和 rename 的 `--target-key` 完全同义 |

### 13.7 修改文件

#### `cmd/find_cmd.go`

```go
// 修改前
cmd.Flags().StringVar(&findKey, "key", "", "Show the data like the key")

// 修改后
cmd.Flags().StringVar(&findKey, "match-key", "", "Show the data like the match key")
```

#### `cmd/unmarsha_cmd.go`

```go
// 修改前
cmd.Flags().StringVar(&unmarshallKey, "key", "", "the proto.marshal value of the full key")

// 修改后
cmd.Flags().StringVar(&unmarshallKey, "target-key", "", "the target key of the etcd data")
```

### 13.8 使用方式变化

```bash
# 修改前
etcdctl+ find --key=qa
etcdctl+ unmarshal --key=/registry/pods/default/my-pod ...

# 修改后
etcdctl+ find --match-key=qa
etcdctl+ unmarshal --target-key=/registry/pods/default/my-pod ...
```
