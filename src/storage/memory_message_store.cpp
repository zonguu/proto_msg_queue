#include "storage/memory_message_store.h"

#include <chrono>
#include <cstring>

namespace pmqueue {

MemoryMessageStore::MemoryMessageStore(size_t default_buffer_size)
    : default_buffer_size_(default_buffer_size) {}

bool MemoryMessageStore::CreateTopic(const TopicName& topic) {
    std::lock_guard<std::mutex> lock(topics_mutex_);
    if (topics_.find(topic) != topics_.end()) {
        return false; // 已存在
    }
    auto data = std::make_unique<TopicData>();
    data->ring_buffer = std::make_unique<SpscRingBuffer>(default_buffer_size_);
    topics_[topic] = std::move(data);
    return true;
}

bool MemoryMessageStore::DeleteTopic(const TopicName& topic) {
    std::lock_guard<std::mutex> lock(topics_mutex_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) {
        return false;
    }
    topics_.erase(it);
    return true;
}

bool MemoryMessageStore::HasTopic(const TopicName& topic) const {
    std::lock_guard<std::mutex> lock(topics_mutex_);
    return topics_.find(topic) != topics_.end();
}

bool MemoryMessageStore::Publish(const TopicName& topic, const Payload& payload, MessageId& out_msg_id) {
    TopicData* topic_data = nullptr;
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        auto it = topics_.find(topic);
        if (it == topics_.end()) {
            // 自动创建 Topic
            auto data = std::make_unique<TopicData>();
            data->ring_buffer = std::make_unique<SpscRingBuffer>(default_buffer_size_);
            topic_data = data.get();
            topics_[topic] = std::move(data);
        } else {
            topic_data = it->second.get();
        }
    }

    // 分配消息 ID
    out_msg_id = topic_data->next_msg_id.fetch_add(1, std::memory_order_relaxed);

    // 序列化：msg_id(8B) + timestamp(8B) + payload_size(4B) + payload(NB)
    const uint64_t timestamp = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    std::vector<uint8_t> serialized;
    serialized.reserve(sizeof(MessageId) + sizeof(uint64_t) + sizeof(uint32_t) + payload.size());

    // msg_id (8 bytes, big-endian)
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((out_msg_id >> (i * 8)) & 0xFF));
    }

    // timestamp (8 bytes, big-endian)
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((timestamp >> (i * 8)) & 0xFF));
    }

    // payload_size (4 bytes, big-endian)
    uint32_t size = static_cast<uint32_t>(payload.size());
    for (int i = 3; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((size >> (i * 8)) & 0xFF));
    }

    // payload
    serialized.insert(serialized.end(), payload.begin(), payload.end());

    return topic_data->ring_buffer->Push(serialized);
}

std::vector<StoredMessage> MemoryMessageStore::Pull(
    const TopicName& topic, const SubscriberId& subscriber_id, uint32_t max_messages) {
    std::vector<StoredMessage> result;

    SpscRingBuffer* ring_buffer = nullptr;
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        auto it = topics_.find(topic);
        if (it == topics_.end()) {
            return result;
        }
        ring_buffer = it->second->ring_buffer.get();
    }

    // 获取当前 offset（这里简化处理，不严格按 offset 过滤）
    for (uint32_t i = 0; i < max_messages; ++i) {
        auto data = ring_buffer->Pop();
        if (!data.has_value()) {
            break;
        }

        const auto& buf = data.value();
        if (buf.size() < sizeof(MessageId) + sizeof(uint64_t) + sizeof(uint32_t)) {
            continue;
        }

        size_t offset = 0;

        // 解析 msg_id
        MessageId msg_id = 0;
        for (size_t j = 0; j < sizeof(MessageId); ++j) {
            msg_id = (msg_id << 8) | buf[offset++];
        }

        // 解析 timestamp
        uint64_t timestamp = 0;
        for (size_t j = 0; j < sizeof(uint64_t); ++j) {
            timestamp = (timestamp << 8) | buf[offset++];
        }

        // 解析 payload_size
        uint32_t payload_size = 0;
        for (size_t j = 0; j < sizeof(uint32_t); ++j) {
            payload_size = (payload_size << 8) | buf[offset++];
        }

        if (buf.size() < offset + payload_size) {
            continue;
        }

        StoredMessage msg;
        msg.id = msg_id;
        msg.topic_name = topic;
        msg.payload.assign(buf.begin() + offset, buf.begin() + offset + payload_size);
        msg.timestamp = timestamp;
        result.push_back(std::move(msg));
    }

    // 更新 offset
    if (!result.empty()) {
        std::lock_guard<std::mutex> lock(offsets_mutex_);
        subscriber_offsets_[MakeOffsetKey(topic, subscriber_id)].last_acked_id = result.back().id;
    }

    return result;
}

bool MemoryMessageStore::Ack(const TopicName& topic, const SubscriberId& subscriber_id, MessageId msg_id) {
    std::lock_guard<std::mutex> lock(offsets_mutex_);
    auto key = MakeOffsetKey(topic, subscriber_id);
    subscriber_offsets_[key].last_acked_id = msg_id;
    return true;
}

} // namespace pmqueue
