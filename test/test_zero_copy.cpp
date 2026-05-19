#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

#include "test_util.h"

using namespace pmqueue;

class ZeroCopyTest : public TestBrokerFixture {
protected:
    void SetUp() override {
        config_.zero_copy_enabled = true;
        TestBrokerFixture::SetUp();
    }
};

TEST_F(ZeroCopyTest, PublishAndResponse) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    auto resp = SendAndWaitResponse(
        client, BuildPublishFrame("zc_topic", "hello zero copy"));

    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    EXPECT_GT(resp->message_id(), 0);
    client.Disconnect();
}

TEST_F(ZeroCopyTest, LargePayloadConsistency) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::string large_payload(100000, 'X');

    auto resp = SendAndWaitResponse(
        client, BuildPublishFrame("zc_large_topic", large_payload));

    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->success());
    client.Disconnect();
}
