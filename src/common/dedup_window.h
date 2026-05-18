#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace pmqueue {

/**
 * @brief 消息去重窗口
 * 
 * 基于生产者序列号的去重机制：
 * - 每个 producer_id 维护已处理的最大 sequence_id
 * - sequence_id <= max_sequence 的消息视为重复
 * - 自动清理不活跃的 producer 记录
 */
class DedupWindow {
public:
    explicit DedupWindow(size_t max_producers = 100000);

    /**
     * @brief 检查是否为重复消息
     * @return true = 重复，false = 新消息
     */
    bool IsDuplicate(const std::string& producer_id, uint64_t sequence_id);

    /**
     * @brief 标记消息已处理
     */
    void MarkProcessed(const std::string& producer_id, uint64_t sequence_id);

    /**
     * @brief 移除指定 producer 的记录
     */
    void RemoveProducer(const std::string& producer_id);

    /**
     * @brief 清理不活跃的 producer 记录
     * @param max_inactive 保留的最大 producer 数量，超出则按 LRU 清理
     */
    void CleanupInactive(size_t max_inactive);

    /**
     * @brief 获取当前 tracked 的 producer 数量
     */
    size_t GetProducerCount() const;

private:
    struct ProducerState {
        uint64_t max_sequence = 0;
        std::chrono::steady_clock::time_point last_active;
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::string, ProducerState> producers_;
    size_t max_producers_;
};

} // namespace pmqueue
