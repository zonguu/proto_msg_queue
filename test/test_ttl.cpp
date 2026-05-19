#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

#include "test_util.h"

using namespace pmqueue;

class TtlTest : public TestBrokerFixture {
protected:
    std::unique_ptr<IMessageStore> CreateStore() override {
        // 使用较短的过期检查间隔，加速测试
        return std::make_unique<MemoryMessageStore>(
            1024 * 1024, 3, 5000, 1000, 500);
    }
};

TEST_F(TtlTest, TtlMessageSkippedInPull) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 发布一条 TTL = 100ms 的消息
    client.SendFrame(
        BuildPublishFrame("ttl_topic", "short_lived", 100));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 拉取消息
    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Push ||
            f.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    client.SendFrame(BuildPullFrame("ttl_topic", "ttl_sub", "", 10));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 消息已过期，不应被拉取到
    EXPECT_EQ(push_count.load(), 0);
    client.Disconnect();
}

TEST_F(TtlTest, NonTtlMessageSurvives) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 发布一条无 TTL 的消息
    client.SendFrame(BuildPublishFrame("no_ttl_topic", "long_lived"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Push ||
            f.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    client.SendFrame(BuildPullFrame("no_ttl_topic", "no_ttl_sub", "", 10));

    EXPECT_TRUE(WaitFor([&]() { return push_count.load() >= 1; }));
    EXPECT_EQ(push_count.load(), 1);
    client.Disconnect();
}

TEST_F(TtlTest, TtlMessageExpiredAndCleaned) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 发布 TTL 消息
    client.SendFrame(
        BuildPublishFrame("clean_topic", "to_be_cleaned", 100));

    // 等待消息过期 + 后台清理线程执行
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // 再次拉取（应无消息）
    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Push ||
            f.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    client.SendFrame(BuildPullFrame("clean_topic", "clean_sub", "", 10));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_EQ(push_count.load(), 0);
    client.Disconnect();
}
