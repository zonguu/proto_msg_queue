#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "common/types.h"
#include "common/non_copyable.h"

namespace pmqueue {

/**
 * @brief 单生产者单消费者无锁环形缓冲区
 * 
 * 基于 std::atomic 的 head/tail 指针，使用 acquire/release 内存序保证线程安全。
 * 生产者（写）和消费者（读）必须在不同线程中运行。
 */
class SpscRingBuffer : public NonCopyable {
public:
    explicit SpscRingBuffer(size_t capacity);
    ~SpscRingBuffer();

    // 生产者接口：写入数据
    bool Push(const uint8_t* data, size_t len);
    bool Push(const std::vector<uint8_t>& data);

    // 消费者接口：读取数据
    std::optional<std::vector<uint8_t>> Pop();

    // 查询状态
    size_t Size() const;
    size_t Capacity() const { return capacity_; }
    bool Empty() const;
    bool Full() const;

private:
    // 内部帮助函数
    size_t GetReadableSize() const;
    size_t GetWritableSize() const;

    const size_t capacity_;      // 缓冲区总容量
    std::unique_ptr<uint8_t[]> buffer_;  // 环形缓冲区

    // 使用 cache line 对齐避免伪共享 (假设 64 字节)
    alignas(64) std::atomic<size_t> head_{0};  // 消费者读位置
    alignas(64) std::atomic<size_t> tail_{0};  // 生产者写位置
};

} // namespace pmqueue
