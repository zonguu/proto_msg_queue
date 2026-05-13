#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <cstring>

#include "ring_buffer/spsc_ring_buffer.h"

using namespace pmqueue;

TEST(SpscRingBufferTest, BasicPushPop) {
    SpscRingBuffer rb(1024);
    EXPECT_TRUE(rb.Empty());
    EXPECT_FALSE(rb.Full());

    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    EXPECT_TRUE(rb.Push(data));
    EXPECT_FALSE(rb.Empty());

    auto result = rb.Pop();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), data);
    EXPECT_TRUE(rb.Empty());
}

TEST(SpscRingBufferTest, PushPopMultiple) {
    SpscRingBuffer rb(64 * 1024);

    for (int i = 0; i < 100; ++i) {
        std::vector<uint8_t> data(100, static_cast<uint8_t>(i));
        EXPECT_TRUE(rb.Push(data));
    }

    for (int i = 0; i < 100; ++i) {
        auto result = rb.Pop();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 100);
        EXPECT_EQ(result.value()[0], static_cast<uint8_t>(i));
    }

    EXPECT_TRUE(rb.Empty());
}

TEST(SpscRingBufferTest, EmptyPopReturnsNullopt) {
    SpscRingBuffer rb(1024);
    auto result = rb.Pop();
    EXPECT_FALSE(result.has_value());
}

TEST(SpscRingBufferTest, FullBufferRejectsPush) {
    SpscRingBuffer rb(64);
    std::vector<uint8_t> large_data(100, 0xAB);
    EXPECT_FALSE(rb.Push(large_data));
}

TEST(SpscRingBufferTest, SpscConcurrency) {
    constexpr int kMessageCount = 10000;
    SpscRingBuffer rb(1024 * 1024);
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};

    std::thread producer([&]() {
        for (int i = 0; i < kMessageCount; ++i) {
            std::vector<uint8_t> data(sizeof(int));
            std::memcpy(data.data(), &i, sizeof(int));
            while (!rb.Push(data)) {
                std::this_thread::yield();
            }
            produced.fetch_add(1);
        }
    });

    std::thread consumer([&]() {
        for (int i = 0; i < kMessageCount; ++i) {
            std::optional<std::vector<uint8_t>> result;
            do {
                result = rb.Pop();
                if (!result) {
                    std::this_thread::yield();
                }
            } while (!result);

            int value = 0;
            std::memcpy(&value, result->data(), sizeof(int));
            EXPECT_EQ(value, i);
            consumed.fetch_add(1);
        }
    });

    producer.join();
    consumer.join();

    EXPECT_EQ(produced.load(), kMessageCount);
    EXPECT_EQ(consumed.load(), kMessageCount);
    EXPECT_TRUE(rb.Empty());
}

TEST(SpscRingBufferTest, WrapAround) {
    SpscRingBuffer rb(256);

    // 写入小数据，填满后消费再写入，测试环绕
    for (int round = 0; round < 10; ++round) {
        for (int i = 0; i < 10; ++i) {
            std::vector<uint8_t> data(10, static_cast<uint8_t>(i));
            EXPECT_TRUE(rb.Push(data));
        }
        for (int i = 0; i < 10; ++i) {
            auto result = rb.Pop();
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result.value()[0], static_cast<uint8_t>(i));
        }
    }
}
