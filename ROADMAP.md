# ProtoMsgQueue 特性路线图

本文档列出当前系统可扩展的特性，按 **重要性** 和 **实现难度** 分类，为后续迭代提供参考。

---

## 核心功能（高优先级）

### 1. 消息持久化（文件/WAL）
- **重要性**: 🔴 高
- **难度**: 🟡 中
- **说明**: 当前为纯内存存储，进程重启后消息丢失。可引入 Write-Ahead Log (WAL) 或分段日志（Segmented Log），按 Topic 分文件存储，支持重启后恢复。
- **涉及模块**: `storage/`, `ring_buffer/`

### 2. 消费者组（Consumer Group）
- **重要性**: 🔴 高
- **难度**: 🟡 中
- **说明**: ✅ **已实现**。当前每个订阅者独立消费同一份消息。消费者组允许多个消费者分摊一个 Topic 的消息，每条消息只被组内一个消费者处理（类似 Kafka 的 Consumer Group）。
- **涉及模块**: `mq/topic_manager`, `mq/broker`, `storage/`

### 3. 消息 ACK + 重试机制
- **重要性**: 🔴 高
- **难度**: 🟢 低
- **说明**: ✅ **已实现**。支持：
  - 未 ACK 的消息进入重试队列
  - 最大重试次数限制
  - 死信队列（DLQ）存放最终失败的消息
- **涉及模块**: `mq/broker`, `storage/message_store`

### 4. 心跳与连接保活
- **重要性**: 🔴 高
- **难度**: 🟢 低
- **说明**: ✅ **已实现**。支持：
  - 客户端定期发送 PING 帧
  - 服务端超时未收到心跳则关闭连接并清理订阅状态
  - 可通过配置 `heartbeat_enabled` 开启/关闭
- **涉及模块**: `network/connection`, `network/tcp_server`, `protocol/`

---

## 性能与扩展（中优先级）

### 5. MPMC 无锁环形缓冲区
- **重要性**: 🟡 中
- **难度**: 🔴 高
- **说明**: 当前 SPSC 队列限制了并发度。替换为多生产者多消费者（MPMC）无锁队列（如基于 FAA + CAS 循环），可大幅提升多线程写入性能。需处理 ABA 问题与伪共享。
- **涉及模块**: `ring_buffer/`

### 6. 零拷贝（Zero-Copy）网络发送
- **重要性**: 🟡 中
- **难度**: 🟡 中
- **说明**: 当前 `Connection::SendFrame` 存在多次内存拷贝（encode → write_buffer → send）。可使用 `sendmsg` + `iovec` 将帧头与 payload 直接组合发送，减少拷贝。
- **涉及模块**: `network/connection`, `protocol/frame_codec`

### 7. 批量读写（Batching）
- **重要性**: 🟡 中
- **难度**: 🟢 低
- **说明**: ✅ **已实现**。支持：
  - **生产端**: 客户端累积多条消息一次性发送（`BatchPublish`），减少系统调用
  - **消费端**: Broker 批量推送消息给订阅者（`BatchPush`），降低网络往返
  - 可通过配置 `batch_publish_enabled` / `batch_push_enabled` 开启/关闭
- **涉及模块**: `network/tcp_client`, `mq/broker`, `protocol/`

### 8. 背压与限流（Backpressure / Flow Control）
- **重要性**: 🟡 中
- **难度**: 🟡 中
- **说明**: ✅ **已实现**。支持：
  - 基于 Ring Buffer 剩余容量的发布背压（返回 buffer full 错误）
  - 单连接令牌桶限流 + 全局令牌桶限流
  - 写缓冲区上限控制（默认 8MB）
  - 可通过配置 `rate_limit_enabled` 开启/关闭
- **涉及模块**: `ring_buffer/`, `mq/broker`, `network/`

---

## 可靠性与运维（中优先级）

### 9. 消息压缩（Compression）
- **重要性**: 🟡 中
- **难度**: 🟢 低
- **说明**: ✅ **已实现**。基于 zlib（deflate）对 payload 进行压缩，减少网络带宽和存储占用。Protobuf 中增加 `compressed` 字段标识。支持：
  - 全局压缩开关 + 单 Topic 压缩开关
  - 压缩阈值控制（超过指定大小才压缩）
  - 可通过配置 `compression_enabled` 开启/关闭
- **涉及模块**: `protocol/frame_codec`, `storage/`, `common/compression`

### 10. 消息过期与 TTL
- **重要性**: 🟡 中
- **难度**: 🟢 低
- **说明**: ✅ **已实现**。支持：
  - 为消息设置存活时间（`ttl_ms`），过期后自动清理
  - 消费端自动跳过过期消息
  - 后台线程定期清理过期消息（`ExpirationLoop`）
  - 可通过配置 `ttl_enabled` 开启/关闭
- **涉及模块**: `storage/memory_message_store`, `mq/broker`

### 11. 消息去重（Exactly-Once）
- **重要性**: 🟡 中
- **难度**: 🔴 高
- **说明**: 网络重试可能导致消息重复投递。实现方案：
  - 生产者端幂等序列号（Idempotent Producer）
  - 服务端维护已处理消息 ID 的布隆过滤器或去重窗口
- **涉及模块**: `mq/broker`, `storage/`, `network/tcp_client`

### 12. 配置化与动态 Topic 参数
- **重要性**: 🟡 中
- **难度**: 🟢 低
- **说明**: ✅ **已实现**。支持：
  - `BrokerConfig` 全局配置结构体（所有特性开关 + 参数）
  - `TopicConfig` Topic 级配置覆盖（ring_buffer_size、ttl、compression）
  - 从 JSON 配置文件加载 / 保存配置
  - 命令行 `--config <file>` 启动
- **涉及模块**: `storage/`, `mq/topic_manager`, `common/config`

---

## 高级功能（低优先级 / 探索性）

### 13. 多副本与高可用（Replication）
- **重要性**: 🟢 低
- **难度**: 🔴 高
- **说明**: 单机部署存在单点故障。引入 Leader-Follower 副本机制，主节点负责写入，从节点异步复制。Leader 宕机时自动选举新 Leader（基于 Raft 或简化版仲裁）。
- **涉及模块**: 新增 `replication/` 模块

### 14. 分区（Partitioning）
- **重要性**: 🟢 低
- **难度**: 🔴 高
- **说明**: 单个 Topic 的消息分散到多个分片（Partition），每个分片独立存储和并行消费。需要引入分区路由策略（Hash / Round-Robin / Range）。
- **涉及模块**: `mq/broker`, `storage/`, `common/`

### 15. 监控与指标（Metrics）
- **重要性**: 🟢 低
- **难度**: 🟢 低
- **说明**: 暴露关键指标：
  - QPS、吞吐量、延迟分位值（P50/P99）
  - Topic 积压消息数、消费者 lag
  - 连接数、内存占用
  - 可通过 Prometheus 格式 / HTTP 接口暴露
- **涉及模块**: 新增 `metrics/` 模块

### 16. 事务消息（Transactions）
- **重要性**: 🟢 低
- **难度**: 🔴 高
- **说明**: 支持跨 Topic 的原子发布（要么全部成功，要么全部失败）。需要两阶段提交（2PC）协议，对性能影响较大。
- **涉及模块**: `mq/broker`, `storage/`, `protocol/`

### 17. 跨语言客户端 SDK
- **重要性**: 🟢 低
- **难度**: 🟡 中
- **说明**: 当前仅有 C++ 客户端。可为 Go、Python、Java 等语言提供 SDK，复用相同的 Protobuf 协议和帧格式。
- **涉及模块**: 新增 `sdk/` 目录

### 18. 管理接口（Admin API / CLI）
- **重要性**: 🟢 低
- **难度**: 🟢 低
- **说明**: 提供查询 Topic 列表、消费者状态、手动清理消息、强制删除 Topic 等管理功能。可基于独立的管理端口或嵌入现有协议。
- **涉及模块**: `mq/broker`, `network/`

---

## 技术债与优化

| 事项 | 优先级 | 说明 |
|------|--------|------|
| 清理 `TcpServer` 日志输出 | 低 | 当前 `Stop()` 有多余日志，应改为可选的 DEBUG 级别 |
| `FrameCodec` 支持流式解码 | 中 | 当前 `TryDecode` 需要完整 buffer，可优化为状态机流式解析 |
| 统一错误码体系 | 中 | 当前错误通过字符串传递，应定义标准化错误码枚举 |
| 单元测试覆盖率 | 中 | 当前测试覆盖核心路径，可补充边界条件和异常注入测试 |

---

## 推荐迭代顺序

```
第一阶段（稳定核心）:
  消息 ACK + 重试 ✅ → 心跳保活 ✅ → 消息持久化

第二阶段（提升性能）:
  批量读写 ✅ → MPMC 无锁队列 → 零拷贝 → 背压限流 ✅

第三阶段（生产就绪）:
  消费者组 ✅ → 消息压缩 ✅ → TTL ✅ → 配置化 ✅ → 监控指标

第四阶段（分布式）:
  分区 → 多副本/高可用 → 事务消息
```
