#include "storage/memory_message_store.h"

#include <cstring>
#include <iostream>

namespace pmqueue {

// ============================================================================
// 构造函数 / 析构函数
// ============================================================================

MemoryMessageStore::MemoryMessageStore(
    size_t default_buffer_size,
    uint32_t max_retry_count,
    uint32_t pending_timeout_ms,
    uint32_t retry_interval_ms,
    uint32_t expiration_check_interval_ms)
    : default_buffer_size_(default_buffer_size)
    , max_retry_count_(max_retry_count)
    , pending_timeout_ms_(pending_timeout_ms)
    , retry_interval_ms_(retry_interval_ms)
    , expiration_check_interval_ms_(expiration_check_interval_ms)
{
    // 启动后台重试线程和过期清理线程
    retry_thread_ = std::thread(&MemoryMessageStore::RetryLoop, this);
    expiration_thread_ = std::thread(&MemoryMessageStore::ExpirationLoop, this);
}

MemoryMessageStore::~MemoryMessageStore() {
    {
        std::lock_guard<std::mutex> lock(retry_mutex_);
        stop_background_threads_ = true;
    }
    retry_cv_.notify_all();
    expiration_cv_.notify_all();
    if (retry_thread_.joinable()) {
        retry_thread_.join();
    }
    if (expiration_thread_.joinable()) {
        expiration_thread_.join();
    }
}

// ============================================================================
// Topic 管理
// ============================================================================

bool MemoryMessageStore::CreateTopic(const TopicName& topic) {
    std::lock_guard<std::mutex> lock(topics_mutex_);
    if (topics_.find(topic) != topics_.end()) {
        return false; // 已存在
    }
    auto data = std::make_unique<TopicData>(default_buffer_size_);
    data->topic_name = topic;
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
    // 同时清理 DLQ
    std::lock_guard<std::mutex> dlq_lock(dlq_mutex_);
    dlq_storage_.erase(MakeDlqName(topic));
    return true;
}

bool MemoryMessageStore::HasTopic(const TopicName& topic) const {
    std::lock_guard<std::mutex> lock(topics_mutex_);
    return topics_.find(topic) != topics_.end();
}

// ============================================================================
// 消息发布
// ============================================================================

bool MemoryMessageStore::Publish(const TopicName& topic, const Payload& payload, MessageId& out_msg_id, uint32_t ttl_ms) {
    TopicData* topic_data = nullptr;
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        auto it = topics_.find(topic);
        if (it == topics_.end()) {
            // 自动创建 Topic
            auto data = std::make_unique<TopicData>(default_buffer_size_);
            data->topic_name = topic;
            topic_data = data.get();
            topics_[topic] = std::move(data);
        } else {
            topic_data = it->second.get();
        }
    }

    // 分配消息 ID（原子操作，无锁）
    out_msg_id = topic_data->next_msg_id.fetch_add(1, std::memory_order_relaxed);

    // 计算过期时间
    const uint64_t expires_at = (ttl_ms > 0) ? (GetCurrentTimeMs() + ttl_ms) : 0;

    // 序列化消息：msg_id(8B) + timestamp(8B) + expires_at(8B) + payload_size(4B) + payload(NB)
    const uint64_t timestamp = GetCurrentTimeMs();

    std::vector<uint8_t> serialized;
    serialized.reserve(sizeof(MessageId) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) + payload.size());

    // msg_id (8 bytes, big-endian)
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((out_msg_id >> (i * 8)) & 0xFF));
    }

    // timestamp (8 bytes, big-endian)
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((timestamp >> (i * 8)) & 0xFF));
    }

    // expires_at (8 bytes, big-endian)
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((expires_at >> (i * 8)) & 0xFF));
    }

    // payload_size (4 bytes, big-endian)
    uint32_t size = static_cast<uint32_t>(payload.size());
    for (int i = 3; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((size >> (i * 8)) & 0xFF));
    }

    // payload
    serialized.insert(serialized.end(), payload.begin(), payload.end());

    // 写入无锁 Ring Buffer
    return topic_data->ring_buffer->Push(serialized);
}

// ============================================================================
// 消息拉取
// ============================================================================

std::vector<StoredMessage> MemoryMessageStore::Pull(
    const TopicName& topic,
    const std::string& consumer_id,
    uint32_t max_messages,
    bool is_group) {
    
    std::vector<StoredMessage> result;
    TopicData* topic_data = nullptr;
    
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        auto it = topics_.find(topic);
        if (it == topics_.end()) {
            return result; // Topic 不存在
        }
        topic_data = it->second.get();
    }

    // 加锁保护消息日志和 offset
    std::lock_guard<std::mutex> lock(topic_data->mutex);

    // 先将 Ring Buffer 中的新消息 drain 到日志
    DrainRingBuffer(*topic_data);

    // 获取当前 offset
    const std::string consumer_key = MakeConsumerKey(is_group, consumer_id);
    MessageId& offset = is_group 
        ? topic_data->group_offsets[consumer_key] 
        : topic_data->subscriber_offsets[consumer_key];
    
    if (offset == 0) {
        offset = 1; // 从第一条消息开始
    }

    const uint64_t now_ms = GetCurrentTimeMs();

    // 从主日志中拉取消息（按 offset 顺序），跳过过期消息
    for (const auto& msg : topic_data->message_log) {
        if (result.size() >= max_messages) {
            break;
        }
        if (msg.id >= offset) {
            // 检查是否已过期
            if (msg.expires_at > 0 && now_ms > msg.expires_at) {
                // 过期消息，直接推进 offset
                offset = msg.id + 1;
                continue;
            }
            // 检查是否已经在 pending 中（避免重复投递）
            auto& pending_map = topic_data->pending_acks[consumer_key];
            if (pending_map.find(msg.id) == pending_map.end()) {
                result.push_back(msg);
                pending_map[msg.id] = PendingMessage{
                    msg,
                    std::chrono::steady_clock::now()
                };
                // 推进 offset：该消息已分配，下一条从新位置开始
                offset = msg.id + 1;
            }
        }
    }
    
    // 从重试队列中拉取消息（不检查 offset，因为重试消息可能 ID 较小）
    // 注意：重试消息是 per-consumer 的，消费后从 retry_queue 中移除
    for (auto it = topic_data->retry_queue.begin(); it != topic_data->retry_queue.end(); ) {
        if (result.size() >= max_messages) {
            break;
        }
        // 跳过过期重试消息
        if (it->expires_at > 0 && now_ms > it->expires_at) {
            it = topic_data->retry_queue.erase(it);
            continue;
        }
        auto& pending_map = topic_data->pending_acks[consumer_key];
        if (pending_map.find(it->id) == pending_map.end()) {
            result.push_back(*it);
            pending_map[it->id] = PendingMessage{
                *it,
                std::chrono::steady_clock::now()
            };
            it = topic_data->retry_queue.erase(it);
        } else {
            ++it;
        }
    }

    return result;
}

// ============================================================================
// 消息确认
// ============================================================================

bool MemoryMessageStore::Ack(
    const TopicName& topic,
    const std::string& consumer_id,
    MessageId msg_id,
    bool is_group) {
    
    TopicData* topic_data = nullptr;
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        auto it = topics_.find(topic);
        if (it == topics_.end()) {
            return false;
        }
        topic_data = it->second.get();
    }

    std::lock_guard<std::mutex> lock(topic_data->mutex);
    const std::string consumer_key = MakeConsumerKey(is_group, consumer_id);
    
    auto& pending_map = topic_data->pending_acks[consumer_key];
    auto it = pending_map.find(msg_id);
    if (it == pending_map.end()) {
        return false; // 消息不在 pending 列表中
    }

    // 确认成功，从 pending 中移除
    pending_map.erase(it);
    return true;
}

// ============================================================================
// 死信队列
// ============================================================================

std::vector<StoredMessage> MemoryMessageStore::PullDlq(
    const TopicName& topic,
    uint32_t max_messages) {
    
    std::vector<StoredMessage> result;
    std::lock_guard<std::mutex> lock(dlq_mutex_);
    
    auto it = dlq_storage_.find(MakeDlqName(topic));
    if (it == dlq_storage_.end()) {
        return result;
    }

    auto& dlq = it->second;
    const size_t count = std::min(static_cast<size_t>(max_messages), dlq.size());
    for (size_t i = 0; i < count; ++i) {
        result.push_back(dlq[i]);
    }
    
    // 移除已拉取的消息
    dlq.erase(dlq.begin(), dlq.begin() + count);
    return result;
}

// ============================================================================
// 内部工具函数
// ============================================================================

void MemoryMessageStore::DrainRingBuffer(TopicData& topic_data) {
    while (true) {
        auto data_opt = topic_data.ring_buffer->Pop();
        if (!data_opt.has_value()) {
            break;
        }

        const auto& buf = data_opt.value();
        // msg_id(8) + timestamp(8) + expires_at(8) + payload_size(4)
        if (buf.size() < sizeof(MessageId) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t)) {
            continue; // 数据损坏，跳过
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

        // 解析 expires_at
        uint64_t expires_at = 0;
        for (size_t j = 0; j < sizeof(uint64_t); ++j) {
            expires_at = (expires_at << 8) | buf[offset++];
        }

        // 解析 payload_size
        uint32_t payload_size = 0;
        for (size_t j = 0; j < sizeof(uint32_t); ++j) {
            payload_size = (payload_size << 8) | buf[offset++];
        }

        if (buf.size() < offset + payload_size) {
            continue; // 数据不足
        }

        StoredMessage msg;
        msg.id = msg_id;
        msg.topic_name = ""; // 在日志中不重复存储 topic 名称以节省内存
        msg.payload.assign(buf.begin() + offset, buf.begin() + offset + payload_size);
        msg.timestamp = timestamp;
        msg.retry_count = 0;
        msg.expires_at = expires_at;

        topic_data.message_log.push_back(std::move(msg));
    }
}

void MemoryMessageStore::RetryLoop() {
    while (true) {
        std::unique_lock<std::mutex> lock(retry_mutex_);
        retry_cv_.wait_for(lock, std::chrono::milliseconds(retry_interval_ms_), [this] {
            return stop_background_threads_.load();
        });
        
        if (stop_background_threads_.load()) {
            break;
        }

        // 扫描所有 Topic 的 pending 消息
        std::vector<TopicData*> topic_data_list;
        {
            std::lock_guard<std::mutex> topics_lock(topics_mutex_);
            for (auto& [name, data] : topics_) {
                topic_data_list.push_back(data.get());
            }
        }

        for (auto* topic_data : topic_data_list) {
            ProcessPendingRetries(*topic_data);
        }
    }
}

void MemoryMessageStore::ExpirationLoop() {
    while (true) {
        std::unique_lock<std::mutex> lock(expiration_mutex_);
        expiration_cv_.wait_for(lock, std::chrono::milliseconds(expiration_check_interval_ms_), [this] {
            return stop_background_threads_.load();
        });
        
        if (stop_background_threads_.load()) {
            break;
        }

        // 扫描所有 Topic 的过期消息
        std::vector<TopicData*> topic_data_list;
        {
            std::lock_guard<std::mutex> topics_lock(topics_mutex_);
            for (auto& [name, data] : topics_) {
                topic_data_list.push_back(data.get());
            }
        }

        for (auto* topic_data : topic_data_list) {
            ExpireTopicMessages(*topic_data);
        }
    }
}

void MemoryMessageStore::ProcessPendingRetries(TopicData& topic_data) {
    std::lock_guard<std::mutex> lock(topic_data.mutex);
    
    const auto now = std::chrono::steady_clock::now();
    const uint64_t now_ms = GetCurrentTimeMs();
    
    // 遍历所有消费者的 pending 消息
    std::vector<std::string> empty_consumer_keys;
    for (auto& [consumer_key, pending_map] : topic_data.pending_acks) {
        std::vector<MessageId> to_remove;
        
        for (auto& [msg_id, pending] : pending_map) {
            const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - pending.pull_time).count();
            
            if (elapsed_ms >= static_cast<int64_t>(pending_timeout_ms_)) {
                // 超时未 ACK
                to_remove.push_back(msg_id);
                
                // 检查消息是否已过期
                if (pending.msg.expires_at > 0 && now_ms > pending.msg.expires_at) {
                    // 已过期，不进入重试队列，直接丢弃
                    continue;
                }
                
                if (pending.msg.retry_count >= max_retry_count_) {
                    // 超过最大重试次数，转入 DLQ
                    StoredMessage dlq_msg = pending.msg;
                    dlq_msg.retry_count = pending.msg.retry_count;
                    {
                        std::lock_guard<std::mutex> dlq_lock(dlq_mutex_);
                        dlq_storage_[MakeDlqName(topic_data.topic_name)].push_back(std::move(dlq_msg));
                    }
                } else {
                    // 放入重试队列（增加重试计数）
                    // 先移除同一 msg_id 的旧重试记录，避免队列中积累多个版本
                    topic_data.retry_queue.erase(
                        std::remove_if(topic_data.retry_queue.begin(), topic_data.retry_queue.end(),
                            [msg_id](const StoredMessage& m) { return m.id == msg_id; }),
                        topic_data.retry_queue.end());
                    StoredMessage retry_msg = pending.msg;
                    retry_msg.retry_count = pending.msg.retry_count + 1;
                    topic_data.retry_queue.push_back(std::move(retry_msg));
                }
            }
        }
        
        // 从 pending 中移除已处理的消息
        for (MessageId msg_id : to_remove) {
            pending_map.erase(msg_id);
        }
        
        if (pending_map.empty()) {
            empty_consumer_keys.push_back(consumer_key);
        }
    }
    
    for (const auto& key : empty_consumer_keys) {
        topic_data.pending_acks.erase(key);
    }
}

void MemoryMessageStore::ExpireTopicMessages(TopicData& topic_data) {
    std::lock_guard<std::mutex> lock(topic_data.mutex);
    
    const uint64_t now_ms = GetCurrentTimeMs();
    
    // 清理 message_log 中的过期消息（重建 deque，保留未过期消息）
    std::deque<StoredMessage> new_log;
    for (auto& msg : topic_data.message_log) {
        if (msg.expires_at == 0 || now_ms <= msg.expires_at) {
            new_log.push_back(std::move(msg));
        }
    }
    topic_data.message_log = std::move(new_log);
    
    // 清理 retry_queue 中的过期消息
    topic_data.retry_queue.erase(
        std::remove_if(topic_data.retry_queue.begin(), topic_data.retry_queue.end(),
            [now_ms](const StoredMessage& m) { return m.expires_at > 0 && now_ms > m.expires_at; }),
        topic_data.retry_queue.end());
    
    // 清理 pending_acks 中的过期消息（视为自动丢弃）
    std::vector<std::string> empty_consumer_keys;
    for (auto& [consumer_key, pending_map] : topic_data.pending_acks) {
        std::vector<MessageId> to_remove;
        for (auto& [msg_id, pending] : pending_map) {
            if (pending.msg.expires_at > 0 && now_ms > pending.msg.expires_at) {
                to_remove.push_back(msg_id);
            }
        }
        for (MessageId msg_id : to_remove) {
            pending_map.erase(msg_id);
        }
        if (pending_map.empty()) {
            empty_consumer_keys.push_back(consumer_key);
        }
    }
    for (const auto& key : empty_consumer_keys) {
        topic_data.pending_acks.erase(key);
    }
}

TopicName MemoryMessageStore::MakeDlqName(const TopicName& topic) {
    return "__dlq." + topic;
}

std::string MemoryMessageStore::MakeConsumerKey(bool is_group, const std::string& id) {
    return is_group ? ("grp:" + id) : ("sub:" + id);
}

uint64_t MemoryMessageStore::GetCurrentTimeMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

bool MemoryMessageStore::IsMessageExpired(const StoredMessage& msg) {
    if (msg.expires_at == 0) {
        return false;
    }
    return GetCurrentTimeMs() > msg.expires_at;
}

} // namespace pmqueue
