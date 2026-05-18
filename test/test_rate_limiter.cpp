#include <gtest/gtest.h>
#include <thread>
#include <chrono>

#include "common/rate_limiter.h"

using namespace pmqueue;

TEST(RateLimiterTest, BasicAcquire) {
    TokenBucket bucket(10, 5); // 10/sec, burst 5
    
    // 初始应有 5 个令牌
    EXPECT_TRUE(bucket.Acquire(1));
    EXPECT_TRUE(bucket.Acquire(1));
    EXPECT_TRUE(bucket.Acquire(1));
    EXPECT_TRUE(bucket.Acquire(1));
    EXPECT_TRUE(bucket.Acquire(1));
    
    // 令牌耗尽
    EXPECT_FALSE(bucket.Acquire(1));
}

TEST(RateLimiterTest, RefillOverTime) {
    TokenBucket bucket(100, 1); // 100/sec, burst 1
    
    EXPECT_TRUE(bucket.Acquire(1));
    EXPECT_FALSE(bucket.Acquire(1));
    
    // 等待 20ms，应产生约 2 个令牌
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_TRUE(bucket.Acquire(1));
}

TEST(RateLimiterTest, BurstCapacity) {
    TokenBucket bucket(1000, 100); // 1000/sec, burst 100
    
    // 一次性消耗 100 个
    EXPECT_TRUE(bucket.Acquire(100));
    EXPECT_FALSE(bucket.Acquire(1));
}

TEST(RateLimiterTest, SetRate) {
    TokenBucket bucket(10, 10);
    EXPECT_TRUE(bucket.Acquire(10));
    
    // SetRate 会触发 refill，但时间差为 0，所以 tokens 不会增加
    // 将 burst 提升到 1000 后，tokens 从 0 被限制到不超过 1000
    bucket.SetRate(1000, 1000);
    EXPECT_FALSE(bucket.Acquire(100)); // 仍然为 0
    
    // 等待一段时间让令牌补充
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    EXPECT_TRUE(bucket.Acquire(100)); // 约产生 120 个令牌
}
