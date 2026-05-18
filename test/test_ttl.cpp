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

class TtlTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 使用较短的过期检查间隔，加速测试
        auto store = std::make_unique<MemoryMessageStore>(
            1024 * 1024, 3, 5000, 1000, 500);
        broker_ = std::make_unique<Broker>(std::move(store), 19093);
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

TEST_F(TtlTest, TtlMessageSkippedInPull) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19093));

    // 发布一条 TTL = 100ms 的消息
    pmqueue::PublishRequest req;
    req.set_topic("ttl_topic");
    req.set_payload("short_lived");
    req.set_ttl_ms(100);

    std::string data;
    req.SerializeToString(&data);
    Frame frame;
    frame.msg_type = FrameMessageType::Publish;
    frame.payload.assign(data.begin(), data.end());
    client.SendFrame(frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 拉取消息
    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Push || f.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    pmqueue::PullRequest pull_req;
    pull_req.set_topic("ttl_topic");
    pull_req.set_subscriber_id("ttl_sub");
    pull_req.set_max_messages(10);

    std::string pull_data;
    pull_req.SerializeToString(&pull_data);
    Frame pull_frame;
    pull_frame.msg_type = FrameMessageType::Pull;
    pull_frame.payload.assign(pull_data.begin(), pull_data.end());
    client.SendFrame(pull_frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 消息已过期，不应被拉取到
    EXPECT_EQ(push_count.load(), 0);
    client.Disconnect();
}

TEST_F(TtlTest, NonTtlMessageSurvives) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19093));

    // 发布一条无 TTL 的消息
    pmqueue::PublishRequest req;
    req.set_topic("no_ttl_topic");
    req.set_payload("long_lived");
    // ttl_ms 默认为 0

    std::string data;
    req.SerializeToString(&data);
    Frame frame;
    frame.msg_type = FrameMessageType::Publish;
    frame.payload.assign(data.begin(), data.end());
    client.SendFrame(frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Push || f.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    pmqueue::PullRequest pull_req;
    pull_req.set_topic("no_ttl_topic");
    pull_req.set_subscriber_id("no_ttl_sub");
    pull_req.set_max_messages(10);

    std::string pull_data;
    pull_req.SerializeToString(&pull_data);
    Frame pull_frame;
    pull_frame.msg_type = FrameMessageType::Pull;
    pull_frame.payload.assign(pull_data.begin(), pull_data.end());
    client.SendFrame(pull_frame);

    for (int i = 0; i < 50 && push_count.load() < 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(push_count.load(), 1);
    client.Disconnect();
}

TEST_F(TtlTest, TtlMessageExpiredAndCleaned) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19093));

    // 发布 TTL 消息
    pmqueue::PublishRequest req;
    req.set_topic("clean_topic");
    req.set_payload("to_be_cleaned");
    req.set_ttl_ms(100);

    std::string data;
    req.SerializeToString(&data);
    Frame frame;
    frame.msg_type = FrameMessageType::Publish;
    frame.payload.assign(data.begin(), data.end());
    client.SendFrame(frame);

    // 等待消息过期 + 后台清理线程执行
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // 再次拉取（应无消息）
    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Push || f.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    pmqueue::PullRequest pull_req;
    pull_req.set_topic("clean_topic");
    pull_req.set_subscriber_id("clean_sub");
    pull_req.set_max_messages(10);

    std::string pull_data;
    pull_req.SerializeToString(&pull_data);
    Frame pull_frame;
    pull_frame.msg_type = FrameMessageType::Pull;
    pull_frame.payload.assign(pull_data.begin(), pull_data.end());
    client.SendFrame(pull_frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_EQ(push_count.load(), 0);
    client.Disconnect();
}
