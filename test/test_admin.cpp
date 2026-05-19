#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

#include "test_util.h"

using namespace pmqueue;

class AdminTest : public TestBrokerFixture {};

TEST_F(AdminTest, ListTopicsEmpty) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_LIST_TOPICS);
    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_NE(resp->json_result().find("topics"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, ListTopicsWithContent) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 先订阅一个 topic 使其存在
    client.SendFrame(
        BuildSubscribeFrame("admin_test_topic", "admin_sub_1"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_LIST_TOPICS);
    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_NE(resp->json_result().find("admin_test_topic"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, GetTopicInfo) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 创建 topic
    client.SendFrame(BuildSubscribeFrame("info_topic", "info_sub"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_GET_TOPIC_INFO, "info_topic");
    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_NE(resp->json_result().find("info_topic"), std::string::npos);
    EXPECT_NE(resp->json_result().find("exists"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, GetTopicInfoMissingTopic) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_GET_TOPIC_INFO);
    ASSERT_TRUE(resp.has_value());
    EXPECT_FALSE(resp->success());
    client.Disconnect();
}

TEST_F(AdminTest, DeleteTopic) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    // 创建并删除
    client.SendFrame(BuildSubscribeFrame("del_topic", "del_sub"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_DELETE_TOPIC, "del_topic");
    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_NE(resp->json_result().find("deleted"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, GetStats) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_GET_STATS);
    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_NE(resp->json_result().find("topics"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, GetConnections) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_GET_CONNECTIONS);
    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_NE(resp->json_result().find("connections"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, CleanupTopic) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_CLEANUP_TOPIC, "cleanup_topic");
    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_NE(resp->json_result().find("cleaned"), std::string::npos);
    client.Disconnect();
}

TEST_F(AdminTest, CleanupTopicMissingTopic) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_CLEANUP_TOPIC);
    ASSERT_TRUE(resp.has_value());
    EXPECT_FALSE(resp->success());
    client.Disconnect();
}

TEST_F(AdminTest, UnknownCommand) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAdminCommand(client, pmqueue::ADMIN_UNKNOWN);

    ASSERT_TRUE(resp.has_value());
    EXPECT_FALSE(resp->success());
    client.Disconnect();
}
