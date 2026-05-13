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

class IntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto store = std::make_unique<MemoryMessageStore>(1024 * 1024);
        broker_ = std::make_unique<Broker>(std::move(store), 19090);
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

TEST_F(IntegrationTest, ClientConnect) {
    TcpClient client;
    EXPECT_TRUE(client.Connect("127.0.0.1", 19090));
    EXPECT_TRUE(client.IsConnected());
    client.Disconnect();
    EXPECT_FALSE(client.IsConnected());
}

TEST_F(IntegrationTest, PublishAndResponse) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19090));

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

    pmqueue::PublishRequest req;
    req.set_topic("test_topic");
    req.set_payload("hello world");

    std::string data;
    req.SerializeToString(&data);

    Frame frame;
    frame.msg_type = FrameMessageType::Publish;
    frame.payload.assign(data.begin(), data.end());

    EXPECT_TRUE(client.SendFrame(frame));

    // 等待响应
    for (int i = 0; i < 50 && !received_response.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(received_response.load());
    client.Disconnect();
}

TEST_F(IntegrationTest, SubscribeAndPush) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19090));

    std::atomic<bool> received_push{false};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push) {
            pmqueue::PushMessage push;
            if (push.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                EXPECT_EQ(push.topic(), "push_topic");
                EXPECT_EQ(push.payload(), "push_data");
                received_push = true;
            }
        }
    });

    // 先订阅
    pmqueue::SubscribeRequest sub_req;
    sub_req.set_topic("push_topic");
    sub_req.set_subscriber_id("sub1");

    std::string sub_data;
    sub_req.SerializeToString(&sub_data);

    Frame sub_frame;
    sub_frame.msg_type = FrameMessageType::Subscribe;
    sub_frame.payload.assign(sub_data.begin(), sub_data.end());
    client.SendFrame(sub_frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 再发布
    pmqueue::PublishRequest pub_req;
    pub_req.set_topic("push_topic");
    pub_req.set_payload("push_data");

    std::string pub_data;
    pub_req.SerializeToString(&pub_data);

    Frame pub_frame;
    pub_frame.msg_type = FrameMessageType::Publish;
    pub_frame.payload.assign(pub_data.begin(), pub_data.end());
    client.SendFrame(pub_frame);

    // 等待推送
    for (int i = 0; i < 50 && !received_push.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(received_push.load());
    client.Disconnect();
}

TEST_F(IntegrationTest, PullMessages) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19090));

    // 先发布一些消息
    for (int i = 0; i < 5; ++i) {
        pmqueue::PublishRequest req;
        req.set_topic("pull_topic");
        req.set_payload("msg" + std::to_string(i));

        std::string data;
        req.SerializeToString(&data);

        Frame frame;
        frame.msg_type = FrameMessageType::Publish;
        frame.payload.assign(data.begin(), data.end());
        client.SendFrame(frame);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 拉取消息
    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push) {
            push_count.fetch_add(1);
        }
    });

    pmqueue::PullRequest pull_req;
    pull_req.set_topic("pull_topic");
    pull_req.set_subscriber_id("pull_sub");
    pull_req.set_max_messages(10);

    std::string pull_data;
    pull_req.SerializeToString(&pull_data);

    Frame pull_frame;
    pull_frame.msg_type = FrameMessageType::Pull;
    pull_frame.payload.assign(pull_data.begin(), pull_data.end());
    client.SendFrame(pull_frame);

    // 等待拉取结果
    for (int i = 0; i < 50 && push_count.load() < 5; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(push_count.load(), 5);
    client.Disconnect();
}

TEST_F(IntegrationTest, MultipleClients) {
    constexpr int kClientCount = 5;
    std::vector<std::unique_ptr<TcpClient>> clients;
    std::atomic<int> total_pushes{0};

    for (int i = 0; i < kClientCount; ++i) {
        auto client = std::make_unique<TcpClient>();
        ASSERT_TRUE(client->Connect("127.0.0.1", 19090));

        client->SetFrameHandler([&total_pushes](const Frame& frame) {
            if (frame.msg_type == FrameMessageType::Push) {
                total_pushes.fetch_add(1);
            }
        });

        // 订阅
        pmqueue::SubscribeRequest sub_req;
        sub_req.set_topic("multi_topic");
        sub_req.set_subscriber_id("sub" + std::to_string(i));

        std::string sub_data;
        sub_req.SerializeToString(&sub_data);

        Frame sub_frame;
        sub_frame.msg_type = FrameMessageType::Subscribe;
        sub_frame.payload.assign(sub_data.begin(), sub_data.end());
        client->SendFrame(sub_frame);

        clients.push_back(std::move(client));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 发布消息
    auto pub_client = std::make_unique<TcpClient>();
    ASSERT_TRUE(pub_client->Connect("127.0.0.1", 19090));

    pmqueue::PublishRequest pub_req;
    pub_req.set_topic("multi_topic");
    pub_req.set_payload("broadcast");

    std::string pub_data;
    pub_req.SerializeToString(&pub_data);

    Frame pub_frame;
    pub_frame.msg_type = FrameMessageType::Publish;
    pub_frame.payload.assign(pub_data.begin(), pub_data.end());
    pub_client->SendFrame(pub_frame);

    // 等待所有客户端收到推送
    for (int i = 0; i < 50 && total_pushes.load() < kClientCount; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(total_pushes.load(), kClientCount);

    for (auto& client : clients) {
        client->Disconnect();
    }
    pub_client->Disconnect();
}
