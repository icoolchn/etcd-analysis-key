# etcd-analysis 子命令速查表

## 子命令总览

| 命令 | 功能 | 操作类型 | flag | 可选参数 |
|------|------|---------|------|---------|
| **distribute** | 数据大小分布分析 | 只读 | `--type` | `key`（默认）、`value`、`kv` |
| | | | `--bucket` | 整数（默认 `5`） |
| | | | `--write-out` | `text`（默认）、`json` |
| **look** | 查看/导出全量数据 | 只读 | `--show-value` | `true`、`false`（默认） |
| | | | `--write-out` | `stdout`（默认）、`file`、`log` |
| | | | `--hang` | `true`、`false`（默认） |
| | | | `--hang-interval` | 整数，单位秒（默认 `2`） |
| | | | `--filter` | `none`（默认）、`key`、`value`、`kv` |
| | | | `--filter-min` | 整数，单位字节（默认 `-1` 不限制） |
| | | | `--filter-max` | 整数，单位字节（默认 `-1` 不限制） |
| **find** | 按关键字搜索 key | 只读 | `--match-key` | 字符串（模糊匹配） |
| | | | `--prefix` | 字符串（key 前缀） |
| | | | `--value` | `true`、`false`（默认） |
| | | | `--limit` | 整数（默认 `10`） |
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
| `--endpoints` | `127.0.0.1:2379` | 逗号分隔的地址列表 | etcd 集群地址 |
| `--cert` | 空 | 文件路径 | TLS 客户端证书文件 |
| `--key` | 空 | 文件路径 | TLS 客户端私钥文件 |
| `--cacert` | 空 | 文件路径 | TLS CA 证书文件 |
| `--command-timeout` | `5` | 整数，单位秒 | 操作超时时间 |

## 使用示例

### distribute

```bash
# 按 key 大小分布（默认）
etcdctl+ distribute

# 按 value 大小分布，8 个桶
etcdctl+ distribute --type=value --bucket=8

# 按 key+value 合计大小分布
etcdctl+ distribute --type=kv

# JSON 格式输出（方便程序调用）
etcdctl+ distribute --type=value --write-out=json

# JSON 配合 jq 查询
etcdctl+ distribute --type=value --write-out=json | jq '.summary.largest_bytes'
```

### look

```bash
# 终端查看（默认不显示 value）
etcdctl+ look

# 显示 value
etcdctl+ look --show-value

# 导出到文件
etcdctl+ look --write-out=file

# 持续监听，每 5 秒刷新
etcdctl+ look --write-out=file --hang=true --hang-interval=5

# 只看 key 大小在 74~100 字节之间的数据
etcdctl+ look --filter=key --filter-min=74 --filter-max=100

# 日志格式输出（适合 loki）,"log" 和 "file" 均是文件写入
etcdctl+ look --write-out=log


# 配合管道
etcdctl+ look | more
```

> ⚠️ **性能提示：** look 会全量读取 etcd 所有数据，数据量大时对集群读压力较大。建议先用 `distribute --type=kv` 查看数据总量和条数，数据量大时（几十万条+）谨慎使用 look，避免对生产集群造成过大读压力。

### find

```bash
# 搜索包含 "index" 的 key
etcdctl+ find --match-key=index

# 按前缀搜索
etcdctl+ find --prefix=/registry/pods

# 搜索并显示 value
etcdctl+ find --match-key=index --value

# 限制返回数量
etcdctl+ find --match-key=index --limit=50
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
# 所有命令都支持全局 TLS 参数
etcdctl+ \
  --endpoints=https://etcd.example.com:2379 \
  --cert=/path/to/client.pem \
  --key=/path/to/client-key.pem \
  --cacert=/path/to/ca.pem \
  distribute --type=value
```
