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

class AdminTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.port = 19094;
        config_.heartbeat_enabled = false;
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

    BrokerConfig config_;
    std::unique_ptr<Broker> broker_;
};

static pmqueue::AdminResponse SendAdminCommand(TcpClient& client,
                                                pmqueue::AdminCommandType cmd,
                                                const std::string& topic = "") {
    pmqueue::AdminRequest req;
    req.set_command(cmd);
    if (!topic.empty()) {
        req.set_topic(topic);
    }

    std::string data;
    req.SerializeToString(&data);
    Frame frame;
    frame.msg_type = FrameMessageType::Admin;
    frame.payload.assign(data.begin(), data.end());
    client.SendFrame(frame);

    pmqueue::AdminResponse resp;
    std::atomic<bool> received{false};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Admin) {
            if (resp.ParseFromArray(f.payload.data(), static_cast<int>(f.payload.size()))) {
                received.store(true);
            }
        }
    });

    for (int i = 0; i < 50 && !received.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return resp;
}

TEST_F(AdminTest, ListTopicsEmpty) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_LIST_TOPICS);
    EXPECT_TRUE(resp.success());
    EXPECT_NE(resp.json_result().find("topics"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, ListTopicsWithContent) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    // 先订阅一个 topic 使其存在
    pmqueue::SubscribeRequest sub_req;
    sub_req.set_topic("admin_test_topic");
    sub_req.set_subscriber_id("admin_sub_1");
    std::string sub_data;
    sub_req.SerializeToString(&sub_data);
    Frame sub_frame;
    sub_frame.msg_type = FrameMessageType::Subscribe;
    sub_frame.payload.assign(sub_data.begin(), sub_data.end());
    client.SendFrame(sub_frame);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_LIST_TOPICS);
    EXPECT_TRUE(resp.success());
    EXPECT_NE(resp.json_result().find("admin_test_topic"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, GetTopicInfo) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    // 创建 topic
    pmqueue::SubscribeRequest sub_req;
    sub_req.set_topic("info_topic");
    sub_req.set_subscriber_id("info_sub");
    std::string sub_data;
    sub_req.SerializeToString(&sub_data);
    Frame sub_frame;
    sub_frame.msg_type = FrameMessageType::Subscribe;
    sub_frame.payload.assign(sub_data.begin(), sub_data.end());
    client.SendFrame(sub_frame);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_GET_TOPIC_INFO, "info_topic");
    EXPECT_TRUE(resp.success());
    EXPECT_NE(resp.json_result().find("info_topic"), std::string::npos);
    EXPECT_NE(resp.json_result().find("exists"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, GetTopicInfoMissingTopic) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_GET_TOPIC_INFO);
    EXPECT_FALSE(resp.success());
    client.Disconnect();
}

TEST_F(AdminTest, DeleteTopic) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    // 创建并删除
    pmqueue::SubscribeRequest sub_req;
    sub_req.set_topic("del_topic");
    sub_req.set_subscriber_id("del_sub");
    std::string sub_data;
    sub_req.SerializeToString(&sub_data);
    Frame sub_frame;
    sub_frame.msg_type = FrameMessageType::Subscribe;
    sub_frame.payload.assign(sub_data.begin(), sub_data.end());
    client.SendFrame(sub_frame);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_DELETE_TOPIC, "del_topic");
    EXPECT_TRUE(resp.success());
    EXPECT_NE(resp.json_result().find("deleted"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, GetStats) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_GET_STATS);
    EXPECT_TRUE(resp.success());
    EXPECT_NE(resp.json_result().find("topics"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, GetConnections) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_GET_CONNECTIONS);
    EXPECT_TRUE(resp.success());
    EXPECT_NE(resp.json_result().find("connections"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, CleanupTopic) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_CLEANUP_TOPIC, "cleanup_topic");
    EXPECT_TRUE(resp.success());
    EXPECT_NE(resp.json_result().find("cleaned"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, CleanupTopicMissingTopic) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_CLEANUP_TOPIC);
    EXPECT_FALSE(resp.success());
    client.Disconnect();
}

TEST_F(AdminTest, UnknownCommand) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094, &config_));

    pmqueue::AdminRequest req;
    req.set_command(pmqueue::ADMIN_UNKNOWN);
    std::string data;
    req.SerializeToString(&data);
    Frame frame;
    frame.msg_type = FrameMessageType::Admin;
    frame.payload.assign(data.begin(), data.end());
    client.SendFrame(frame);

    pmqueue::AdminResponse resp;
    std::atomic<bool> received{false};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Admin) {
            if (resp.ParseFromArray(f.payload.data(), static_cast<int>(f.payload.size()))) {
                received.store(true);
            }
        }
    });

    for (int i = 0; i < 50 && !received.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    EXPECT_FALSE(resp.success());
    client.Disconnect();
}
