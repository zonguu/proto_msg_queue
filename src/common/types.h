#pragma once

#include <cstdint>
#include <string>
#include <functional>
#include <vector>

namespace pmqueue {

// Topic 类型 - 使用 std::string
using TopicName = std::string;
using SubscriberId = std::string;
using MessageId = uint64_t;
using Payload = std::vector<uint8_t>;

// 消息回调
using MessageCallback = std::function<void(MessageId, const TopicName&, const Payload&)>;

// 连接ID
using ConnectionId = uint64_t;

constexpr uint32_t kDefaultRingBufferSize = 1024 * 1024; // 1MB
constexpr uint32_t kMaxPayloadSize = 16 * 1024 * 1024;   // 16MB

// 心跳与连接保活默认值
constexpr uint32_t kDefaultPingIntervalMs = 5000;       // 客户端心跳间隔 5s
constexpr uint32_t kDefaultHeartbeatTimeoutMs = 15000;  // 服务端心跳超时 15s
constexpr uint32_t kHeartbeatCheckIntervalMs = 5000;    // 服务端心跳检查间隔 5s

// 背压与限流默认值
constexpr uint32_t kDefaultGlobalPublishRate = 10000;   // 全局发布限流 10000 msg/s
constexpr uint32_t kDefaultConnPublishRate = 1000;      // 单连接发布限流 1000 msg/s
constexpr uint32_t kDefaultRateLimitBurst = 100;        // 令牌桶突发容量
constexpr size_t kMaxWriteBufferSize = 8 * 1024 * 1024; // 写缓冲区上限 8MB

// TTL 默认值
constexpr uint32_t kDefaultExpirationCheckIntervalMs = 10000; // 过期检查间隔 10s

} // namespace pmqueue
