#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

#include "test_util.h"

using namespace pmqueue;

class BatchPublishTest : public TestBrokerFixture {};

TEST_F(BatchPublishTest, BatchPublishAndResponse) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::vector<std::pair<std::string, std::string>> msgs;
    for (int i = 0; i < 5; ++i) {
        msgs.emplace_back("batch_topic", "msg" + std::to_string(i));
    }

    auto resp = SendAndWaitResponse(
        client, BuildBatchPublishFrame(msgs));

    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_GT(resp->message_id(), 0);
    client.Disconnect();
}

TEST_F(BatchPublishTest, BatchPublishToSubscriber) {
    TcpClient sub_client;
    ASSERT_TRUE(sub_client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<int> received_count{0};
    sub_client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::BatchPush) {
            pmqueue::BatchPushMessage batch;
            if (batch.ParseFromArray(frame.payload.data(),
                                     static_cast<int>(frame.payload.size()))) {
                received_count.fetch_add(batch.messages_size());
            }
        } else if (frame.msg_type == FrameMessageType::Push) {
            received_count.fetch_add(1);
        }
    });

    // 订阅
    sub_client.SendFrame(BuildSubscribeFrame("batch_sub_topic", "sub1"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 批量发布
    TcpClient pub_client;
    ASSERT_TRUE(pub_client.Connect("127.0.0.1", config_.port, &config_));

    std::vector<std::pair<std::string, std::string>> msgs;
    for (int i = 0; i < 3; ++i) {
        msgs.emplace_back("batch_sub_topic", "batch_msg" + std::to_string(i));
    }
    pub_client.SendFrame(BuildBatchPublishFrame(msgs));

    EXPECT_TRUE(WaitFor([&]() { return received_count.load() >= 3; }));
    EXPECT_EQ(received_count.load(), 3);
    pub_client.Disconnect();
    sub_client.Disconnect();
}

TEST_F(BatchPublishTest, PullReturnsBatchPush) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 发布 5 条消息
    for (int i = 0; i < 5; ++i) {
        client.SendFrame(
            BuildPublishFrame("pull_batch_topic", "pull_msg" + std::to_string(i)));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::atomic<int> received_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::BatchPush) {
            pmqueue::BatchPushMessage batch;
            if (batch.ParseFromArray(frame.payload.data(),
                                     static_cast<int>(frame.payload.size()))) {
                received_count.fetch_add(batch.messages_size());
            }
        } else if (frame.msg_type == FrameMessageType::Push) {
            received_count.fetch_add(1);
        }
    });

    client.SendFrame(BuildPullFrame("pull_batch_topic", "pull_sub", "", 10));

    EXPECT_TRUE(WaitFor([&]() { return received_count.load() >= 5; }));
    EXPECT_EQ(received_count.load(), 5);
    client.Disconnect();
}
