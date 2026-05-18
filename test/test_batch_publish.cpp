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

class BatchPublishTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto store = std::make_unique<MemoryMessageStore>(1024 * 1024);
        broker_ = std::make_unique<Broker>(std::move(store), 19091);
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

TEST_F(BatchPublishTest, BatchPublishAndResponse) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19091));

    std::atomic<bool> received_response{false};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                EXPECT_TRUE(resp.success());
                EXPECT_GT(resp.message_id(), 0);
                received_response = true;
            }
        }
    });

    pmqueue::BatchPublishRequest batch_req;
    for (int i = 0; i < 5; ++i) {
        auto* msg = batch_req.add_messages();
        msg->set_topic("batch_topic");
        msg->set_payload("msg" + std::to_string(i));
    }

    std::string data;
    batch_req.SerializeToString(&data);

    Frame frame;
    frame.msg_type = FrameMessageType::BatchPublish;
    frame.payload.assign(data.begin(), data.end());

    EXPECT_TRUE(client.SendFrame(frame));

    for (int i = 0; i < 50 && !received_response.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(received_response.load());
    client.Disconnect();
}

TEST_F(BatchPublishTest, BatchPublishToSubscriber) {
    TcpClient sub_client;
    ASSERT_TRUE(sub_client.Connect("127.0.0.1", 19091));

    std::atomic<int> received_count{0};
    sub_client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::BatchPush) {
            pmqueue::BatchPushMessage batch;
            if (batch.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                received_count.fetch_add(batch.messages_size());
            }
        } else if (frame.msg_type == FrameMessageType::Push) {
            received_count.fetch_add(1);
        }
    });

    // 订阅
    pmqueue::SubscribeRequest sub_req;
    sub_req.set_topic("batch_sub_topic");
    sub_req.set_subscriber_id("sub1");

    std::string sub_data;
    sub_req.SerializeToString(&sub_data);
    Frame sub_frame;
    sub_frame.msg_type = FrameMessageType::Subscribe;
    sub_frame.payload.assign(sub_data.begin(), sub_data.end());
    sub_client.SendFrame(sub_frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 批量发布
    TcpClient pub_client;
    ASSERT_TRUE(pub_client.Connect("127.0.0.1", 19091));

    pmqueue::BatchPublishRequest batch_req;
    for (int i = 0; i < 3; ++i) {
        auto* msg = batch_req.add_messages();
        msg->set_topic("batch_sub_topic");
        msg->set_payload("batch_msg" + std::to_string(i));
    }

    std::string data;
    batch_req.SerializeToString(&data);
    Frame frame;
    frame.msg_type = FrameMessageType::BatchPublish;
    frame.payload.assign(data.begin(), data.end());
    pub_client.SendFrame(frame);

    for (int i = 0; i < 50 && received_count.load() < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(received_count.load(), 3);
    pub_client.Disconnect();
    sub_client.Disconnect();
}

TEST_F(BatchPublishTest, PullReturnsBatchPush) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19091));

    // 发布 5 条消息
    for (int i = 0; i < 5; ++i) {
        pmqueue::PublishRequest req;
        req.set_topic("pull_batch_topic");
        req.set_payload("pull_msg" + std::to_string(i));

        std::string data;
        req.SerializeToString(&data);
        Frame frame;
        frame.msg_type = FrameMessageType::Publish;
        frame.payload.assign(data.begin(), data.end());
        client.SendFrame(frame);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::atomic<int> received_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::BatchPush) {
            pmqueue::BatchPushMessage batch;
            if (batch.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                received_count.fetch_add(batch.messages_size());
            }
        } else if (frame.msg_type == FrameMessageType::Push) {
            received_count.fetch_add(1);
        }
    });

    pmqueue::PullRequest pull_req;
    pull_req.set_topic("pull_batch_topic");
    pull_req.set_subscriber_id("pull_sub");
    pull_req.set_max_messages(10);

    std::string pull_data;
    pull_req.SerializeToString(&pull_data);
    Frame pull_frame;
    pull_frame.msg_type = FrameMessageType::Pull;
    pull_frame.payload.assign(pull_data.begin(), pull_data.end());
    client.SendFrame(pull_frame);

    for (int i = 0; i < 50 && received_count.load() < 5; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(received_count.load(), 5);
    client.Disconnect();
}
