#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace pmqueue {

/**
 * @brief 令牌桶限流器
 * 
 * 支持突发流量和匀速限制。
 * 线程安全，可跨线程调用 Acquire。
 */
class TokenBucket {
public:
    /**
     * @param rate_per_second 每秒产生令牌数
     * @param burst_size 桶容量（最大突发数）
     */
    explicit TokenBucket(uint32_t rate_per_second, uint32_t burst_size)
        : rate_per_second_(rate_per_second)
        , burst_size_(burst_size)
        , tokens_(static_cast<double>(burst_size))
        , last_refill_time_(std::chrono::steady_clock::now()) {}

    /**
     * @brief 尝试获取指定数量的令牌
     * @param tokens 需要的令牌数
     * @return true 成功获取，false 被限流
     */
    bool Acquire(uint32_t tokens = 1) {
        std::lock_guard<std::mutex> lock(mutex_);
        Refill();
        if (tokens_ >= static_cast<double>(tokens)) {
            tokens_ -= static_cast<double>(tokens);
            return true;
        }
        return false;
    }

    /**
     * @brief 设置新的速率参数
     */
    void SetRate(uint32_t rate_per_second, uint32_t burst_size) {
        std::lock_guard<std::mutex> lock(mutex_);
        Refill();
        rate_per_second_ = rate_per_second;
        burst_size_ = burst_size;
        if (tokens_ > static_cast<double>(burst_size)) {
            tokens_ = static_cast<double>(burst_size);
        }
    }

private:
    void Refill() {
        auto now = std::chrono::steady_clock::now();
        double elapsed_seconds = std::chrono::duration<double>(now - last_refill_time_).count();
        last_refill_time_ = now;

        tokens_ += elapsed_seconds * static_cast<double>(rate_per_second_);
        if (tokens_ > static_cast<double>(burst_size_)) {
            tokens_ = static_cast<double>(burst_size_);
        }
    }

    uint32_t rate_per_second_;
    uint32_t burst_size_;
    double tokens_;
    std::chrono::steady_clock::time_point last_refill_time_;
    std::mutex mutex_;
};

} // namespace pmqueue
