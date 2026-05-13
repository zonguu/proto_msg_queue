#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/message_store.h"
#include "ring_buffer/spsc_ring_buffer.h"

namespace pmqueue {

/**
 * @brief 基于无锁环形缓冲区的内存消息存储实现
 * 
 * 每个 Topic 对应一个 SPSC 环形缓冲区。
 * 订阅者通过 offset 追踪消费进度。
 */
class MemoryMessageStore : public IMessageStore {
public:
    explicit MemoryMessageStore(size_t default_buffer_size = kDefaultRingBufferSize);
    ~MemoryMessageStore() override = default;

    bool Publish(const TopicName& topic, const Payload& payload, MessageId& out_msg_id) override;
    std::vector<StoredMessage> Pull(const TopicName& topic, const SubscriberId& subscriber_id, uint32_t max_messages) override;
    bool Ack(const TopicName& topic, const SubscriberId& subscriber_id, MessageId msg_id) override;
    bool CreateTopic(const TopicName& topic) override;
    bool DeleteTopic(const TopicName& topic) override;
    bool HasTopic(const TopicName& topic) const override;

private:
    struct TopicData {
        std::unique_ptr<SpscRingBuffer> ring_buffer;
        std::atomic<MessageId> next_msg_id{1};
    };

    struct SubscriberOffset {
        MessageId last_acked_id{0};
    };

    mutable std::mutex topics_mutex_;
    std::unordered_map<TopicName, std::unique_ptr<TopicData>> topics_;

    mutable std::mutex offsets_mutex_;
    std::unordered_map<std::string, SubscriberOffset> subscriber_offsets_; // key: topic + ":" + subscriber_id

    const size_t default_buffer_size_;

    std::string MakeOffsetKey(const TopicName& topic, const SubscriberId& id) const {
        return topic + ":" + id;
    }
};

} // namespace pmqueue
