#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "storage/message_store.h"
#include "ring_buffer/spsc_ring_buffer.h"

namespace pmqueue {

/**
 * @brief 基于无锁环形缓冲区的内存消息存储实现（支持 ACK、重试、DLQ、消费者组）
 * 
 * 架构：
 * - 写入路径：生产者 -> SPSC Ring Buffer（无锁）-> 后台线程 drain 到消息日志
 * - 读取路径：消费者从消息日志按 offset 拉取，消息 ACK 后才推进 offset
 * - 重试：未 ACK 消息进入 pending 队列，超时后自动重试（重新变为可读）
 * - DLQ：超过最大重试次数的消息转入死信队列
 * - 消费者组：组内所有消费者共享同一个 offset，消息只被组内一个消费者消费
 */
class MemoryMessageStore : public IMessageStore {
public:
    /**
     * @param default_buffer_size Ring Buffer 默认容量
     * @param max_retry_count 单条消息最大重试次数（默认 3）
     * @param pending_timeout_ms pending 消息超时时间（默认 5000ms）
     * @param retry_interval_ms 重试扫描间隔（默认 1000ms）
     */
    explicit MemoryMessageStore(
        size_t default_buffer_size = kDefaultRingBufferSize,
        uint32_t max_retry_count = 3,
        uint32_t pending_timeout_ms = 5000,
        uint32_t retry_interval_ms = 1000);
    
    ~MemoryMessageStore() override;

    bool Publish(const TopicName& topic, const Payload& payload, MessageId& out_msg_id) override;
    
    std::vector<StoredMessage> Pull(
        const TopicName& topic, 
        const std::string& consumer_id, 
        uint32_t max_messages,
        bool is_group) override;
    
    bool Ack(
        const TopicName& topic, 
        const std::string& consumer_id, 
        MessageId msg_id,
        bool is_group) override;
    
    bool CreateTopic(const TopicName& topic) override;
    bool DeleteTopic(const TopicName& topic) override;
    bool HasTopic(const TopicName& topic) const override;
    
    std::vector<StoredMessage> PullDlq(
        const TopicName& topic, 
        uint32_t max_messages) override;

private:
    /**
     * @brief 待确认消息记录
     */
    struct PendingMessage {
        StoredMessage msg;
        std::chrono::steady_clock::time_point pull_time;
    };

    /**
     * @brief Topic 内部数据结构
     */
    struct TopicData {
        TopicName topic_name;  // Topic 名称，用于 DLQ 等场景
        
        // 无锁写入缓冲：生产者直接写入，无需加锁
        std::unique_ptr<SpscRingBuffer> ring_buffer;
        
        // 消息主日志（由 DrainRingBuffer 写入，由 Pull 读取）
        std::deque<StoredMessage> message_log;
        
        // 重试队列：超时未 ACK 的消息重新放入这里，供再次消费
        std::deque<StoredMessage> retry_queue;
        
        // 独立消费者的 offset：subscriber_id -> 下一个应消费的消息 ID
        std::unordered_map<std::string, MessageId> subscriber_offsets;
        
        // 消费者组的 offset：group_id -> 下一个应消费的消息 ID
        std::unordered_map<std::string, MessageId> group_offsets;
        
        // 待确认消息：consumer_id -> {msg_id -> PendingMessage}
        // consumer_id 格式："sub:{id}" 或 "grp:{id}"
        std::unordered_map<std::string, std::unordered_map<MessageId, PendingMessage>> pending_acks;
        
        // 用于保护 message_log / offsets / pending_acks 的互斥锁
        std::mutex mutex;
        
        std::atomic<MessageId> next_msg_id{1};
        
        explicit TopicData(size_t buffer_size) 
            : ring_buffer(std::make_unique<SpscRingBuffer>(buffer_size)) {}
    };

    mutable std::mutex topics_mutex_;
    std::unordered_map<TopicName, std::unique_ptr<TopicData>> topics_;

    // DLQ 存储：key = "__dlq.{topic_name}"
    mutable std::mutex dlq_mutex_;
    std::unordered_map<TopicName, std::deque<StoredMessage>> dlq_storage_;

    const size_t default_buffer_size_;
    const uint32_t max_retry_count_;
    const uint32_t pending_timeout_ms_;
    const uint32_t retry_interval_ms_;

    std::atomic<bool> stop_retry_thread_{false};
    std::thread retry_thread_;
    std::condition_variable retry_cv_;
    std::mutex retry_mutex_;

    // 将 Ring Buffer 中的消息 drain 到消息日志
    void DrainRingBuffer(TopicData& topic_data);
    
    // 后台重试线程入口
    void RetryLoop();
    
    // 检查并处理一个 Topic 的 pending 超时消息
    void ProcessPendingRetries(TopicData& topic_data);
    
    // 构造 DLQ Topic 名称
    static TopicName MakeDlqName(const TopicName& topic);
    
    // 构造 consumer key
    static std::string MakeConsumerKey(bool is_group, const std::string& id);
};

} // namespace pmqueue
