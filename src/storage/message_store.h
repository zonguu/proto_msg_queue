#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/types.h"
#include "mq/topic.h"

namespace pmqueue {

// 存储的消息结构
struct StoredMessage {
    MessageId id;
    TopicName topic_name;
    Payload payload;
    uint64_t timestamp;
};

/**
 * @brief 消息存储层抽象接口
 * 
 * 网络层通过此接口与存储层交互，实现解耦。
 */
class IMessageStore {
public:
    virtual ~IMessageStore() = default;

    // 发布消息到指定 Topic
    virtual bool Publish(const TopicName& topic, const Payload& payload, MessageId& out_msg_id) = 0;

    // 拉取消息
    virtual std::vector<StoredMessage> Pull(const TopicName& topic, const SubscriberId& subscriber_id, uint32_t max_messages) = 0;

    // 确认消费
    virtual bool Ack(const TopicName& topic, const SubscriberId& subscriber_id, MessageId msg_id) = 0;

    // 创建 Topic
    virtual bool CreateTopic(const TopicName& topic) = 0;

    // 删除 Topic
    virtual bool DeleteTopic(const TopicName& topic) = 0;

    // 检查 Topic 是否存在
    virtual bool HasTopic(const TopicName& topic) const = 0;
};

} // namespace pmqueue
