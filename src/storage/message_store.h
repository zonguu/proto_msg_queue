#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/types.h"
#include "mq/topic.h"

namespace pmqueue {

/**
 * @brief 消息消费状态
 */
enum class ConsumeStatus {
    Pending,    // 已投递，等待 ACK
    Acked,      // 已确认
    Failed,     // 达到最大重试次数，转入 DLQ
};

// 存储的消息结构
struct StoredMessage {
    MessageId id;
    TopicName topic_name;
    Payload payload;
    uint64_t timestamp;
    uint32_t retry_count = 0;  // 当前重试次数
    uint64_t expires_at = 0;   // 过期时间戳（毫秒级 Unix 时间，0 表示永不过期）
};

/**
 * @brief 消息存储层抽象接口
 * 
 * 网络层通过此接口与存储层交互，实现解耦。
 * 支持：
 * - 发布/拉取/ACK
 * - 消费者组级别的消息隔离
 * - 消息重试与死信队列（DLQ）
 */
class IMessageStore {
public:
    virtual ~IMessageStore() = default;

    /**
     * @brief 发布消息到指定 Topic
     */
    virtual bool Publish(const TopicName& topic, const Payload& payload, MessageId& out_msg_id, uint32_t ttl_ms = 0) = 0;

    /**
     * @brief 拉取消息
     * @param topic Topic 名称
     * @param consumer_id 消费者 ID（独立消费者）或组 ID（消费者组）
     * @param max_messages 最大拉取条数
     * @param is_group 是否为消费者组拉取
     */
    virtual std::vector<StoredMessage> Pull(
        const TopicName& topic, 
        const std::string& consumer_id, 
        uint32_t max_messages,
        bool is_group) = 0;

    /**
     * @brief 确认消费
     * @param topic Topic 名称
     * @param consumer_id 消费者 ID 或组 ID
     * @param msg_id 消息 ID
     * @param is_group 是否为消费者组 ACK
     */
    virtual bool Ack(
        const TopicName& topic, 
        const std::string& consumer_id, 
        MessageId msg_id,
        bool is_group) = 0;

    /**
     * @brief 创建 Topic
     */
    virtual bool CreateTopic(const TopicName& topic) = 0;

    /**
     * @brief 删除 Topic
     */
    virtual bool DeleteTopic(const TopicName& topic) = 0;

    /**
     * @brief 检查 Topic 是否存在
     */
    virtual bool HasTopic(const TopicName& topic) const = 0;

    /**
     * @brief 从死信队列拉取消息
     * @param topic 原始 Topic 名称（DLQ 名称为内部构造）
     */
    virtual std::vector<StoredMessage> PullDlq(
        const TopicName& topic, 
        uint32_t max_messages) = 0;
};

} // namespace pmqueue
