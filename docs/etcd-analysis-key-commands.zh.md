# etcd-analysis 子命令速查表

## 子命令总览

| 命令 | 功能 | 操作类型 | flag | 可选参数 |
|------|------|---------|------|---------|
| **distribute** | 数据大小分布分析 | 只读 | `--type` | `key`（默认）、`value`、`kv` |
| | | | `--bucket` | 整数（默认 `5`） |
| | | | `--write-out` | `text`（默认）、`json` |
| | | | `--prefix` | 字符串（server-side 前缀扫描） |
| | | | `--page-size` | 整数（默认 `1000`，每页 key 数） |
| | | | `--page-sleep` | duration（默认 `0`，如 `50ms`） |
| **look** | 查看/导出全量数据 | 只读 | `--show-value` | `true`、`false`（默认） |
| | | | `--write-out` | `stdout`（默认）、**`jsonl`（推荐，结构化）**、`file`、`log`（日志采集，非离线分析用） |
| | | | `--output` | 文件路径（`file`/`log`/`jsonl` 写入） |
| | | | `--snapshot` | snapshot db 文件路径（离线一次遍历全字段，含 rev_count/tombstone_count） |
| | | | `--hang` | `true`、`false`（默认） |
| | | | `--hang-interval` | 整数，单位秒（默认 `2`） |
| | | | `--filter` | `none`（默认）、`key`、`value`、`kv`（客户端过滤） |
| | | | `--filter-min` | 整数，单位字节（默认 `-1` 不限制） |
| | | | `--filter-max` | 整数，单位字节（默认 `-1` 不限制） |
| | | | `--keys-only` | `true`、`false`（默认），只拉 key metadata，不拉 value |
| | | | `--prefix` | 字符串（server-side 前缀扫描） |
| | | | `--page-size` | 整数（默认 `1000`） |
| | | | `--page-sleep` | duration（默认 `0`，如 `50ms`） |
| **summary** | 按前缀聚合 Top N | 只读 | `--input` | JSONL 快照文件（离线模式；空=在线扫描） |
| | | | `--keys-only` | `true`、`false`（默认，仅在线模式，不拉 value） |
| | | | `--prefix` | 字符串（仅在线模式，server-side 前缀） |
| | | | `--group-depth` | 整数（默认 `2`，按前 N 段路径聚合） |
| | | | `--strip-suffix` | 字符串（默认空=关闭，如 `.` 去掉最后一段路径里的 `.<uid>` 后缀再分组） |
| | | | `--top` | 整数（默认 `20`，输出前 N 个分组） |
| | | | `--sort` | `count`（默认）、`total-size`、`avg-size`、`max-size`、`max-version`、`latest-mod-revision`、`created-count`、`modified-count`、`distinct-lease-count`、`rev-count`、`tombstone-count` |
| | | | `--min-create-revision` / `--max-create-revision` | int64（默认 `0` 不限制） |
| | | | `--min-mod-revision` / `--max-mod-revision` | int64（默认 `0` 不限制） |
| | | | `--filter` / `--filter-min` / `--filter-max` | 客户端过滤（默认 `none` / `-1` / `-1`） |
| | | | `--page-size` / `--page-sleep` | 仅在线模式 |
| | | | `--write-out` | `text`（默认）、`json` |
| | | | `--output` | 文件路径（默认 stdout） |
| **find** | 按关键字搜索 key | 只读 | `--match-key` | 字符串（模糊匹配） |
| | | | `--prefix` | 字符串（key 前缀） |
| | | | `--value` | `true`、`false`（默认） |
| | | | `--limit` | 整数（默认 `10`，已下推到 etcd server 作为 Range limit） |
| **wal-look** | 导出 WAL 操作流 | 离线只读 | `--data-dir` | etcd data dir（含 `member/wal`）或 WAL 目录（含 `*.wal`）（required） |
| | | | `--write-out` | `stdout`（默认）、**`jsonl`（推荐）**、`log`（日志采集） |
| | | | `--output` | 文件路径 |
| | | | `--start-index` | uint64（默认 `0`，含） |
| | | | `--end-index` | uint64（默认 max，不含） |
| | | | `--entry-type` | 逗号分隔，17 种（`IRRPut,IRRDeleteRange,...`；`-h` 查全部） |
| **wal-summary** | WAL 按 key 聚合写次数 | 离线只读 | `--input` | WalOp JSONL 文件（离线）；空=直接解析 WAL |
| | | | `--data-dir` | etcd data dir 或 WAL 目录（`--input` 为空时 required） |
| | | | `--sort` | `put-count`（默认）、`delete-count`、`total-ops` |
| | | | `--top` | 整数（默认 `20`） |
| | | | `--write-out` | `text`（默认）、`json` |
| | | | `--output` | 文件路径（默认 stdout） |
| **dump** | 原始数据导出 | 离线只读 | 子命令 | `list-bucket`、`iterate-bucket`、`scan-keys`、`wal` |
| | | | `--snapshot` | snapshot db 路径（db 子命令 required） |
| | | | `--data-dir` | etcd data dir 或 WAL 目录（`wal` 子命令 required） |
| | | | `--start-index` / `--end-index` | raft index 范围（`wal` 子命令） |
| | | | `--entry-type` | 逗号分隔，17 种（`wal` 子命令） |
| | | | `--start-revision` / `--end-revision` | revision 范围（`scan-keys`） |
| | | | `--decode` / `--limit` | 解码为 KeyValue / 限制条数（`iterate-bucket`） |
| **unmarshal** | Proto 反序列化 | 只读 | `--target-key` | 字符串（etcd 完整 key） |
| | | | `--import-path` | 字符串，可指定多个 |
| | | | `--proto` | 字符串（.proto 文件路径），可指定多个 |
| | | | `--full-message-name` | 字符串（`包名.消息名`） |
| **leader** | 查询 leader 节点 | 只读 | 无 | — |
| **decode** | Base64 解码 | 纯本地 | `--value` | 字符串（base64 编码值） |
| **clear** | 清空所有数据 | 🔴 写操作（已禁用） | 无 | — |
| **rename** | 重命名 key | 🟠 写操作（已禁用） | `--source-key` | 字符串（原 key） |
| | | | `--target-key` | 字符串（新 key） |
| | | | `--bak` | `true`（默认）、`false` |
| **completion** | Shell 自动补全 | 纯本地 | 子命令 | `bash`、`zsh`、`fish`、`powershell` |

## 全局 flag（所有命令共享）

| flag | 默认值 | 可选参数 | 说明 |
|------|--------|---------|------|
| `--endpoints` | `127.0.0.1:2379` | 逗号分隔的地址列表 | etcd 集群地址；排查生产建议只传一个 follower |
| `--cert` | 空 | 文件路径 | TLS 客户端证书文件 |
| `--key` | 空 | 文件路径 | TLS 客户端私钥文件 |
| `--cacert` | 空 | 文件路径 | TLS CA 证书文件 |
| `--command-timeout` | `5` | 整数，单位秒 | 操作超时时间 |

> ⚠️ **TLS 证书验证现状：** 当前只要配置了 `--cert`/`--key`/`--cacert`，就会无条件设置 `InsecureSkipVerify=true`（跳过服务端证书验证）。自签证书场景下可用，但存在中间人风险。后续计划改为显式 `--insecure-skip-tls-verify` flag（默认严格验证），目前尚未落地。

## 使用示例

### distribute

```bash
# 按 key 大小分布（默认，text 输出）
etcdctl+ distribute

# 按 value 大小分布，8 个桶
etcdctl+ distribute --type=value --bucket=8

# 按 key+value 合计大小分布
etcdctl+ distribute --type=kv

# 只扫某个前缀（server-side，不全量扫）
etcdctl+ distribute --prefix=/registry/events --type=value

# JSON 输出（结构化，方便程序调用 / jq 查询；distribute 仅支持 text/json）
etcdctl+ distribute --type=value --write-out=json
etcdctl+ distribute --type=value --write-out=json | jq '.summary.largest_bytes'

# JSON 落盘供程序消费（distribute 无 jsonl 模式，json 即结构化输出）
etcdctl+ distribute --type=kv --write-out=json --output=distribute.json

# 生产稳妥扫描：调大 page、页间 sleep
etcdctl+ distribute --type=kv --page-size=5000 --page-sleep=50ms
```

### look

```bash
# 导出全量数据为 JSONL（推荐，性能最好，结构化、可被 summary/find/distribute 离线反复分析）
etcdctl+ look --write-out=jsonl --output=keys.jsonl

# keys-only JSONL 快照：只拉 key metadata 不拉 value，最低风险
etcdctl+ look --keys-only --write-out=jsonl --output=keys.jsonl

# 某前缀的全字段 JSONL（含 value size，不含 value 内容）
etcdctl+ look --prefix=/registry/events --write-out=jsonl --output=events-kv-meta.jsonl

# 离线 snapshot db 一次遍历全字段（含 rev_count/tombstone_count，不连 etcd）
etcdctl+ look --snapshot=snapshot.db --write-out=jsonl --output=keys.jsonl

# 终端查看（默认 stdout，不写文件；数据量大时建议改 jsonl 落盘）
etcdctl+ look
etcdctl+ look --show-value                  # 显示 value（base64）
etcdctl+ look | more                        # 配合管道分页

# 持续监听，每 5 秒刷新（仅 file 模式生效）
etcdctl+ look --write-out=file --hang=true --hang-interval=5

# 客户端过滤：key 大小在 74~100 字节之间（仍会拉 value，不减少 server 返回量）
etcdctl+ look --filter=key --filter-min=74 --filter-max=100

# 其他输出格式（非离线分析首选）：
#   --write-out=log   日志格式，适合 loki 等日志系统采集；非结构化，不能被 summary --input 消费
#   --write-out=file  纯文本落盘；结构化分析请用 jsonl
etcdctl+ look --keys-only --write-out=log --output=keys.log

# 配合管道
etcdctl+ look | more
```

> ⚠️ **性能提示：** look 默认会全量读取 etcd 所有数据（含 value）。生产环境优先 `--keys-only --write-out=jsonl`（不拉 value + 结构化落盘），再用 `summary --input=keys.jsonl` 离线反复分析；`--prefix` 限定范围；必须看 value 时再低峰期执行。`--filter` 是客户端过滤，不减少 server 返回量。`--write-out=log/file` 为日志/文本场景，非结构化、不能被离线命令消费，分析用途请用 `jsonl`。

**look log/jsonl 输出字段（拆分 size 后）：**

```text
# 完整模式（含 value）
key=... value=- key_size_bytes=33 value_size_bytes=10240 kv_size_bytes=10273 kv_size_human=10.1KiB create_revision=... mod_revision=... version=... lease=...

# keys-only 模式（省略 value 相关字段）
key=... key_size_bytes=33 create_revision=... mod_revision=... version=... lease=...
```

`--keys-only` 组合规则：`--keys-only --filter=value|kv` 不允许（无 value 无法计算）；`--keys-only --show-value` 不允许（语义冲突）。

### summary

按 key 前缀聚合，输出 Top N 分组。支持在线扫描和离线（`--input` 读 JSONL 快照）两种模式。

```bash
# 在线便捷模式：只拉 key metadata，按前缀聚合 key 数最多的 Top 50
etcdctl+ summary --keys-only --group-depth=2 --sort=count --top=50

# 离线模式：基于 look 导出的 keys-only 快照反复分析，不访问 etcd
etcdctl+ summary --input=keys.jsonl --group-depth=2 --sort=count --top=50

# 高频覆盖写热点前缀（version 最高）
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=max-version --top=50

# 某时间点后修改最多的前缀（revision 来自 Grafana）
etcdctl+ summary --input=keys.jsonl --min-mod-revision=38770000000 --group-depth=3 --sort=count --top=50

# 某时间点后新增最多的前缀
etcdctl+ summary --input=keys.jsonl --min-create-revision=38770000000 --group-depth=3 --sort=count --top=50

# 可疑前缀的空间占用（需要含 value size 的快照）
etcdctl+ summary --input=events-kv-meta.jsonl --group-depth=3 --sort=total-size --top=50

# 在线模式带客户端过滤 + 稳妥扫描
etcdctl+ summary --keys-only --filter=key --filter-min=100 --page-size=1000 --page-sleep=50ms

# JSON 输出便于程序处理（summary 仅支持 text/json，无 jsonl 模式）
etcdctl+ summary --input=keys.jsonl --sort=count --write-out=json --output=summary.json

# K8s <name>.<uid> 类 key 按 <name> 聚合（events/services/endpoints…）
# --strip-suffix=. 只剥最后一段路径里的 .<uid>，中间段（如 monitoring.coreos.com）不动
etcdctl+ summary --input=keys.jsonl --prefix=/registry/events/kyuubi \
    --strip-suffix=. --group-depth=4 --sort=count --top=10

# 按 lease 种类数排序：定位哪些前缀挂的 lease 最杂
etcdctl+ summary --input=keys.jsonl --group-depth=3 --sort=distinct-lease-count --top=10
```

**`--group-depth` 分组规则**（以 `/registry/pods/default/nginx` 为例）：

| depth | 分组前缀 |
|---|---|
| 1 | `/registry` |
| 2 | `/registry/pods` |
| 3 | `/registry/pods/default` |
| 4 | `/registry/pods/default/nginx` |

K8s 场景建议：`--group-depth=2` 看资源类型，`--group-depth=3` 看资源类型 + namespace。

**`--sort` 取值：**

| sort | 用途 |
|---|---|
| `count` | key 数最多的前缀 |
| `total-size` / `avg-size` / `max-size` | 占用空间最大 / 平均对象大 / 单个大对象（需含 value 的快照） |
| `max-version` | 高频覆盖写热点前缀 |
| `latest-mod-revision` | 最近活跃修改的前缀 |
| `created-count` / `modified-count` | 配合 `--min-*-revision` 找某 revision 后新增/修改最多的前缀 |
| `distinct-lease-count` | 组内不同 lease id 数最多的前缀（lease 种类最杂） |
| `rev-count` | 历史 revision 数最多的前缀（仅离线 snapshot JSONL） |
| `tombstone-count` | tombstone 数最多的前缀（仅离线 snapshot JSONL） |

**lease 三列**（常驻显示，与 `distribute` 的 `lease==0 = persistent` 语义一致）：

| 列 | 含义 |
|---|---|
| `key_leased_count` | 组内挂 lease（非零）的 key 数；可加，others 行求和 |
| `distinct_lease_count` | 组内不同 lease id 数（去重）；不可加，others 行显示 `-` |
| `max_lease` | 组内最大 lease id（真实可 `etcdctl lease inspect` 的 id）；不可加，others 行显示 `-` |

`key_leased_count / distinct_lease_count` 即 lease 复用度：≈1 → 每个 key 独占 lease；>>1 → 少量 lease 被大量 key 共用。

**输出示例（text）：**

```text
Summary: 2090000 keys, 15 groups (top 15 by count)

prefix | count | total_size | avg_size | max_size | max_version | latest_mod_revision | created_count | modified_count | key_leased_count | distinct_lease_count | max_lease | rev_count | tombstone_count | percent
/registry/events | 980000 | - | - | - | 3 | 38780000020 | 0 | 0 | 980000 | 980000 | 5821917203594880612 | 980000 | 0 | 99.0%
/registry/pods | 530000 | - | - | - | 1024 | 38780000010 | 0 | 0 | 0 | 0 | 0 | 530000 | 0 | 0.5%
...
```

> keys-only 快照没有 value size，`total_size`/`avg_size`/`max_size` 显示 `-`；`rev_count`/`tombstone_count` 仅离线 snapshot JSONL 有值（在线/普通 keys-only 快照这两列不显示）。

keys-only 快照没有 value size，`TotalSize`/`AvgSize`/`MaxSize` 显示 `-`；用 `look --prefix=... --write-out=jsonl`（不带 `--keys-only`）导出的快照才有 size 列。

### find

```bash
# 搜索包含 "index" 的 key
etcdctl+ find --match-key=index

# 按前缀搜索
etcdctl+ find --prefix=/registry/pods

# 搜索并显示 value
etcdctl+ find --match-key=index --value

# 限制返回数量（已下推到 etcd server，大 prefix 也安全）
etcdctl+ find --match-key=index --limit=50
```

### wal-look

从 etcd 数据目录解析 WAL，导出操作流。**只读不 flock，可直接对生产 `member/wal` 跑**（与官方 `etcd-dump-logs` 同机制，不阻塞集群、不损坏数据）。

```bash
# 导出 WAL 操作流为 JSONL（推荐，结构化，可被 wal-summary --input 反复分析）
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=jsonl --output=wal.jsonl

# --data-dir 支持两种输入：etcd data dir（含 member/wal）或 WAL 目录（含 *.wal）直接传
etcdctl+ wal-look --data-dir=/var/lib/etcd/member/wal --write-out=jsonl --output=wal.jsonl

# 只看某个 index 范围内的操作
etcdctl+ wal-look --data-dir=/var/lib/etcd --start-index=930 --end-index=1000 \
    --write-out=jsonl --output=wal-range.jsonl

# 按操作类型过滤（--entry-type 支持全部 17 种，见下方对应表；-h 可查）
etcdctl+ wal-look --data-dir=/var/lib/etcd --entry-type=IRRPut,IRRDeleteRange \
    --write-out=jsonl --output=wal-writes.jsonl

# 终端查看（默认 stdout）
etcdctl+ wal-look --data-dir=/var/lib/etcd

# 其他输出格式（非离线分析首选）：
#   --write-out=log  日志格式，适合 loki 等日志系统采集；非结构化，不能被 wal-summary --input 消费
etcdctl+ wal-look --data-dir=/var/lib/etcd --write-out=log --output=wal.log
```

JSONL 输出（每行一个 WalOp；无 key/value 的操作如 Compaction 不输出相应字段）：

```json
{"raft_index":10864,"raft_term":3,"op_type":"Put","key":".monitor","value_size_bytes":17,"entry_type":"IRRPut"}
{"raft_index":10865,"raft_term":3,"op_type":"Compaction","entry_type":"IRRCompaction"}
{"raft_index":10866,"raft_term":3,"op_type":"DeleteRange","key":"/old","entry_type":"IRRDeleteRange"}
```

**`op_type` ↔ `entry_type` 对应关系**（`-h` 同样列出）：

| op_type | entry_type | 说明 |
|---|---|---|
| Range | IRRRange | 读请求；正常不写 WAL，出现即异常 |
| Put | IRRPut | 写入 / 覆盖写 |
| DeleteRange | IRRDeleteRange | 范围删除 |
| Txn | IRRTxn | 事务父记录；子操作 entry_type 仍为 IRRTxn，is_txn=true |
| Compaction | IRRCompaction | 压缩；WAL 中 payload 极小 |
| LeaseGrant | IRRLeaseGrant | 授予 lease |
| LeaseRevoke | IRRLeaseRevoke | 撤销 lease |
| LeaseCheckpoint | IRRLeaseCheckpoint | lease 检查点（KeepAlive 不写 WAL） |
| AuthEnable | IRRAuthEnable | 开启鉴权 |
| AuthDisable | IRRAuthDisable | 关闭鉴权 |
| AuthUser | IRRAuthUser | 用户增删 / 改密 / 授权 |
| AuthRole | IRRAuthRole | 角色增删 / 授权 |

另有 5 个兜底类型：`IRRUnknown`、`ConfigChange`、`Normal`、`Request`、`Unknown`。

> 💡 **WAL 写压力归因**：Put + DeleteRange（含 Txn 子操作）是 WAL 体积主体，其他类型合计通常 <5%。Txn 子操作的 `op_type` 为 Put/DeleteRange，已被 wal-summary 计入 PutCount/DeleteCount，不会漏。

### wal-summary

按 key 聚合 WAL 写操作次数，从 WalOp JSONL 或直接解析 WAL。**回答"哪个 key 写最多"**。

```bash
# 离线模式（推荐）：基于 wal-look 导出的 JSONL 反复分析，不重复解析 WAL
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --top=50

# 直接解析 WAL（一步，但每次都重新解析；小集群或一次性查看可用）
etcdctl+ wal-summary --data-dir=/var/lib/etcd --sort=put-count --top=50

# Delete 热点
etcdctl+ wal-summary --input=wal.jsonl --sort=delete-count --top=50

# 按总操作数排序
etcdctl+ wal-summary --input=wal.jsonl --sort=total-ops --top=50

# JSON 输出便于程序处理（wal-summary 仅支持 text/json）
etcdctl+ wal-summary --input=wal.jsonl --sort=put-count --write-out=json --output=wal-summary.json
```

text 输出默认在 Top N 上方打印 **entry-type 分布表**（覆盖全部 17 种类型，回答"操作类型分布"）：

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

`--sort`：`put-count`（默认）/ `delete-count` / `total-ops`。

### dump

从 snapshot db 或 WAL 导出原始条目（纯文本，对齐 `etcd-dump-db`/`etcd-dump-logs` 格式；不走 JSONL 管线）。

```bash
# db: 列 bucket
etcdctl+ dump list-bucket --snapshot=snapshot.db

# db: 逐条导出 bucket 内容（--decode 解码为 mvccpb.KeyValue，--limit 限制条数）
etcdctl+ dump iterate-bucket --snapshot=snapshot.db key --decode --limit=100

# db: 按 revision 范围扫描
etcdctl+ dump scan-keys --snapshot=snapshot.db --start-revision=100 --end-revision=200 --limit=50

# WAL: 原始条目查看（--entry-type 同 wal-look，支持全部 17 种，-h 可查）
etcdctl+ dump wal --data-dir=/var/lib/etcd --entry-type=IRRPut --start-index=930 --end-index=932
```

### unmarshal

```bash
# 解码 etcd 中的 protobuf 数据
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
# 解码 base64 值（纯本地操作，不连接 etcd）
etcdctl+ decode --value="aGVsbG8gd29ybGQ="
```

### TLS 连接示例

```bash
# 所有命令都支持全局 TLS 参数,在线模式
etcdctl+ \
  --endpoints=https://etcd.example.com:2379 \
  --cert=/path/to/client.pem \
  --key=/path/to/client-key.pem \
  --cacert=/path/to/ca.pem \
  distribute --bucket=5
```

```bash
# 使用导入的分析文件，离线模式
./etcdctl+-linux-amd64 \
  --input=analysis.jsonl \
  distribute --bucket=5
```
