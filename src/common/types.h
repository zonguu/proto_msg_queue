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

// 以下常量已迁移至 BrokerConfig，保留部分底层常量
constexpr uint32_t kDefaultRingBufferSize = 1024 * 1024; // 1MB
constexpr uint32_t kMaxPayloadSize = 16 * 1024 * 1024;   // 16MB

} // namespace pmqueue
