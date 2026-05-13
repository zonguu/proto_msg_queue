#include "ring_buffer/spsc_ring_buffer.h"

#include <algorithm>
#include <cstring>

namespace pmqueue {

SpscRingBuffer::SpscRingBuffer(size_t capacity)
    : capacity_(capacity + 1) { // 多一个位置区分满和空
    buffer_ = std::make_unique<uint8_t[]>(capacity_);
}

SpscRingBuffer::~SpscRingBuffer() = default;

bool SpscRingBuffer::Push(const uint8_t* data, size_t len) {
    if (len == 0 || len > capacity_ - 1) {
        return false;
    }

    const size_t tail = tail_.load(std::memory_order_relaxed);
    const size_t head = head_.load(std::memory_order_acquire);

    // 计算可写空间（需要额外 4 字节存储长度）
    const size_t total_len = sizeof(uint32_t) + len;
    size_t writable = (head + capacity_ - tail - 1) % capacity_;
    if (writable < total_len) {
        return false; // 空间不足
    }

    // 写入长度头（4字节，大端序）
    uint32_t len_be = static_cast<uint32_t>(len);
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        buffer_[(tail + i) % capacity_] = static_cast<uint8_t>((len_be >> (24 - i * 8)) & 0xFF);
    }

    // 写入数据
    for (size_t i = 0; i < len; ++i) {
        buffer_[(tail + sizeof(uint32_t) + i) % capacity_] = data[i];
    }

    // 更新 tail，使用 release 保证前面的写入对消费者可见
    tail_.store((tail + total_len) % capacity_, std::memory_order_release);
    return true;
}

bool SpscRingBuffer::Push(const std::vector<uint8_t>& data) {
    return Push(data.data(), data.size());
}

std::optional<std::vector<uint8_t>> SpscRingBuffer::Pop() {
    const size_t head = head_.load(std::memory_order_relaxed);
    const size_t tail = tail_.load(std::memory_order_acquire);

    if (head == tail) {
        return std::nullopt; // 空
    }

    // 读取长度头
    uint32_t len = 0;
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        len = (len << 8) | buffer_[(head + i) % capacity_];
    }

    if (len == 0 || len > kMaxPayloadSize) {
        return std::nullopt; // 数据损坏
    }

    // 读取数据
    std::vector<uint8_t> data(len);
    for (size_t i = 0; i < len; ++i) {
        data[i] = buffer_[(head + sizeof(uint32_t) + i) % capacity_];
    }

    // 更新 head
    head_.store((head + sizeof(uint32_t) + len) % capacity_, std::memory_order_release);
    return data;
}

size_t SpscRingBuffer::Size() const {
    return GetReadableSize();
}

bool SpscRingBuffer::Empty() const {
    return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
}

bool SpscRingBuffer::Full() const {
    const size_t next_tail = (tail_.load(std::memory_order_relaxed) + 1) % capacity_;
    return next_tail == head_.load(std::memory_order_acquire);
}

size_t SpscRingBuffer::GetReadableSize() const {
    const size_t head = head_.load(std::memory_order_acquire);
    const size_t tail = tail_.load(std::memory_order_acquire);
    return (tail + capacity_ - head) % capacity_;
}

size_t SpscRingBuffer::GetWritableSize() const {
    const size_t head = head_.load(std::memory_order_acquire);
    const size_t tail = tail_.load(std::memory_order_acquire);
    return (head + capacity_ - tail - 1) % capacity_;
}

} // namespace pmqueue
