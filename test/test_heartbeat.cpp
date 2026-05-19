#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

#include "test_util.h"

using namespace pmqueue;

class HeartbeatTest : public ::testing::Test {
protected:
    BrokerConfig config_;
    std::unique_ptr<Broker> broker_;

    void SetUp() override {
        // 启用心跳但缩短间隔，加速测试
        config_.port = AllocateTestPort();
        config_.heartbeat_enabled = true;
        config_.heartbeat_interval_ms = 500;
        config_.heartbeat_timeout_ms = 1500;
        config_.heartbeat_check_interval_ms = 500;
        auto store = std::make_unique<MemoryMessageStore>(1024 * 1024);
        broker_ = std::make_unique<Broker>(std::move(store), config_);
        ASSERT_TRUE(broker_->Start());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override {
        if (broker_) {
            broker_->Stop();
        }
    }
};

TEST_F(HeartbeatTest, PingPongExchange) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<bool> received_pong{false};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Pong) {
            received_pong = true;
        }
    });

    // 客户端自动发 Ping（间隔 500ms），等待收到 Pong
    EXPECT_TRUE(WaitFor([&]() { return received_pong.load(); }, 3000));
    client.Disconnect();
}

TEST_F(HeartbeatTest, SubscriptionCleanupOnDisconnect) {
    TcpClient sub_client;
    ASSERT_TRUE(sub_client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<int> push_count{0};
    sub_client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push ||
            frame.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    // 订阅
    sub_client.SendFrame(BuildSubscribeFrame("hb_topic", "hb_sub"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 断开订阅客户端
    sub_client.Disconnect();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 发布消息
    TcpClient pub_client;
    ASSERT_TRUE(pub_client.Connect("127.0.0.1", config_.port, &config_));
    pub_client.SendFrame(BuildPublishFrame("hb_topic", "after_disconnect"));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 订阅者不应收到消息（已断开并清理订阅）
    EXPECT_EQ(push_count.load(), 0);
    pub_client.Disconnect();
}
