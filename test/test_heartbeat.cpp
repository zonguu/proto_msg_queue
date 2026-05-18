#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

#include "mq/broker.h"
#include "network/tcp_client.h"
#include "storage/memory_message_store.h"
#include "protocol/frame_codec.h"
#include "msg_queue.pb.h"

using namespace pmqueue;

class HeartbeatTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto store = std::make_unique<MemoryMessageStore>(1024 * 1024);
        broker_ = std::make_unique<Broker>(std::move(store), 19092);
        ASSERT_TRUE(broker_->Start());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override {
        if (broker_) {
            broker_->Stop();
        }
    }

    std::unique_ptr<Broker> broker_;
};

TEST_F(HeartbeatTest, PingPongExchange) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19092));

    std::atomic<bool> received_pong{false};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Pong) {
            received_pong = true;
        }
    });

    // 客户端自动发 Ping（间隔 5s），等待收到 Pong
    for (int i = 0; i < 100 && !received_pong.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(received_pong.load());
    client.Disconnect();
}

TEST_F(HeartbeatTest, SubscriptionCleanupOnDisconnect) {
    TcpClient sub_client;
    ASSERT_TRUE(sub_client.Connect("127.0.0.1", 19092));

    std::atomic<int> push_count{0};
    sub_client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push || frame.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    // 订阅
    pmqueue::SubscribeRequest sub_req;
    sub_req.set_topic("hb_topic");
    sub_req.set_subscriber_id("hb_sub");

    std::string sub_data;
    sub_req.SerializeToString(&sub_data);
    Frame sub_frame;
    sub_frame.msg_type = FrameMessageType::Subscribe;
    sub_frame.payload.assign(sub_data.begin(), sub_data.end());
    sub_client.SendFrame(sub_frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 断开订阅客户端
    sub_client.Disconnect();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 发布消息
    TcpClient pub_client;
    ASSERT_TRUE(pub_client.Connect("127.0.0.1", 19092));

    pmqueue::PublishRequest pub_req;
    pub_req.set_topic("hb_topic");
    pub_req.set_payload("after_disconnect");

    std::string pub_data;
    pub_req.SerializeToString(&pub_data);
    Frame pub_frame;
    pub_frame.msg_type = FrameMessageType::Publish;
    pub_frame.payload.assign(pub_data.begin(), pub_data.end());
    pub_client.SendFrame(pub_frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 订阅者不应收到消息（已断开并清理订阅）
    EXPECT_EQ(push_count.load(), 0);
    pub_client.Disconnect();
}
