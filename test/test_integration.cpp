#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

#include "test_util.h"

using namespace pmqueue;

class IntegrationTest : public TestBrokerFixture {};

TEST_F(IntegrationTest, ClientConnect) {
    TcpClient client;
    EXPECT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));
    EXPECT_TRUE(client.IsConnected());
    client.Disconnect();
    EXPECT_FALSE(client.IsConnected());
}

TEST_F(IntegrationTest, PublishAndResponse) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAndWaitResponse(
        client, BuildPublishFrame("test_topic", "hello world"));

    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_GT(resp->message_id(), 0);
    client.Disconnect();
}

TEST_F(IntegrationTest, SubscribeAndPush) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<bool> received_push{false};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push) {
            pmqueue::PushMessage push;
            if (push.ParseFromArray(frame.payload.data(),
                                    static_cast<int>(frame.payload.size()))) {
                EXPECT_EQ(push.topic(), "push_topic");
                EXPECT_EQ(push.payload(), "push_data");
                received_push = true;
            }
        }
    });

    // 先订阅
    client.SendFrame(BuildSubscribeFrame("push_topic", "sub1"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 再发布
    client.SendFrame(BuildPublishFrame("push_topic", "push_data"));

    EXPECT_TRUE(WaitFor([&]() { return received_push.load(); }));
    client.Disconnect();
}

TEST_F(IntegrationTest, PullMessages) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 先发布一些消息
    for (int i = 0; i < 5; ++i) {
        client.SendFrame(
            BuildPublishFrame("pull_topic", "msg" + std::to_string(i)));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 拉取消息
    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push) {
            push_count.fetch_add(1);
        } else if (frame.msg_type == FrameMessageType::BatchPush) {
            pmqueue::BatchPushMessage batch;
            if (batch.ParseFromArray(frame.payload.data(),
                                     static_cast<int>(frame.payload.size()))) {
                push_count.fetch_add(batch.messages_size());
            }
        }
    });

    client.SendFrame(BuildPullFrame("pull_topic", "pull_sub", "", 10));

    EXPECT_TRUE(WaitFor([&]() { return push_count.load() >= 5; }));
    EXPECT_EQ(push_count.load(), 5);
    client.Disconnect();
}

TEST_F(IntegrationTest, MultipleClients) {
    constexpr int kClientCount = 5;
    std::vector<std::unique_ptr<TcpClient>> clients;
    std::atomic<int> total_pushes{0};

    for (int i = 0; i < kClientCount; ++i) {
        auto client = std::make_unique<TcpClient>();
        ASSERT_TRUE(client->Connect("127.0.0.1", config_.port, &config_));

        client->SetFrameHandler([&total_pushes](const Frame& frame) {
            if (frame.msg_type == FrameMessageType::Push) {
                total_pushes.fetch_add(1);
            } else if (frame.msg_type == FrameMessageType::BatchPush) {
                pmqueue::BatchPushMessage batch;
                if (batch.ParseFromArray(
                        frame.payload.data(),
                        static_cast<int>(frame.payload.size()))) {
                    total_pushes.fetch_add(batch.messages_size());
                }
            }
        });

        client->SendFrame(
            BuildSubscribeFrame("multi_topic", "sub" + std::to_string(i)));
        clients.push_back(std::move(client));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 发布消息
    auto pub_client = std::make_unique<TcpClient>();
    ASSERT_TRUE(pub_client->Connect("127.0.0.1", config_.port, &config_));
    pub_client->SendFrame(BuildPublishFrame("multi_topic", "broadcast"));

    EXPECT_TRUE(
        WaitFor([&]() { return total_pushes.load() >= kClientCount; }));
    EXPECT_EQ(total_pushes.load(), kClientCount);

    for (auto& client : clients) {
        client->Disconnect();
    }
    pub_client->Disconnect();
}

// ============================================================================
// 消费者组集成测试
// ============================================================================

TEST_F(IntegrationTest, ConsumerGroupRoundRobin) {
    constexpr int kMemberCount = 3;
    std::vector<std::unique_ptr<TcpClient>> members;
    std::atomic<int> total_received{0};
    std::atomic<int> member_received[3] = {0, 0, 0};

    for (int i = 0; i < kMemberCount; ++i) {
        auto client = std::make_unique<TcpClient>();
        ASSERT_TRUE(client->Connect("127.0.0.1", config_.port, &config_));

        int idx = i;
        client->SetFrameHandler(
            [&total_received, &member_received, idx](const Frame& frame) {
                if (frame.msg_type == FrameMessageType::Push) {
                    total_received.fetch_add(1);
                    member_received[idx].fetch_add(1);
                }
            });

        client->SendFrame(BuildSubscribeFrame(
            "group_topic", "member" + std::to_string(i), "group_a"));
        members.push_back(std::move(client));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 发布 6 条消息
    auto pub_client = std::make_unique<TcpClient>();
    ASSERT_TRUE(pub_client->Connect("127.0.0.1", config_.port, &config_));

    for (int i = 0; i < 6; ++i) {
        pub_client->SendFrame(
            BuildPublishFrame("group_topic", "msg" + std::to_string(i)));
    }

    // 等待消息分发
    EXPECT_TRUE(WaitFor([&]() { return total_received.load() >= 6; }));
    EXPECT_EQ(total_received.load(), 6);

    // 每个成员应该收到至少 1 条
    for (int i = 0; i < kMemberCount; ++i) {
        EXPECT_GE(member_received[i].load(), 1);
    }

    for (auto& client : members) {
        client->Disconnect();
    }
    pub_client->Disconnect();
}

TEST_F(IntegrationTest, ConsumerGroupPullAndAck) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 以消费者组成员身份订阅
    client.SendFrame(
        BuildSubscribeFrame("pull_group_topic", "member1", "group_pull"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 发布消息
    client.SendFrame(BuildPublishFrame("pull_group_topic", "group_msg"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 以消费者组身份拉取
    std::atomic<MessageId> received_msg_id{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push) {
            pmqueue::PushMessage push;
            if (push.ParseFromArray(frame.payload.data(),
                                    static_cast<int>(frame.payload.size()))) {
                received_msg_id.store(push.message_id());
            }
        }
    });

    client.SendFrame(
        BuildPullFrame("pull_group_topic", "", "group_pull", 1));

    EXPECT_TRUE(WaitFor([&]() { return received_msg_id.load() > 0; }));
    ASSERT_GT(received_msg_id.load(), 0);

    // ACK 消息
    client.SendFrame(
        BuildAckFrame("pull_group_topic", "", received_msg_id.load(),
                      "group_pull"));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    client.Disconnect();
}
