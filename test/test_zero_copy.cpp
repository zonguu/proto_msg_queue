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

class ZeroCopyTest : public ::testing::Test {
protected:
    void SetUp() override {
        BrokerConfig config;
        config.zero_copy_enabled = true;
        config.port = 19094;
        auto store = std::make_unique<MemoryMessageStore>(1024 * 1024);
        broker_ = std::make_unique<Broker>(std::move(store), config);
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

TEST_F(ZeroCopyTest, PublishAndResponse) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094));

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
    req.set_topic("zc_topic");
    req.set_payload("hello zero copy");

    std::string data;
    req.SerializeToString(&data);

    Frame frame;
    frame.msg_type = FrameMessageType::Publish;
    frame.payload.assign(data.begin(), data.end());

    EXPECT_TRUE(client.SendFrame(frame));

    for (int i = 0; i < 50 && !received_response.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(received_response.load());
    client.Disconnect();
}

TEST_F(ZeroCopyTest, LargePayloadConsistency) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19094));

    std::string large_payload(100000, 'X');

    std::atomic<bool> received_response{false};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                EXPECT_TRUE(resp.success());
                received_response = true;
            }
        }
    });

    pmqueue::PublishRequest req;
    req.set_topic("zc_large_topic");
    req.set_payload(large_payload);

    std::string data;
    req.SerializeToString(&data);

    Frame frame;
    frame.msg_type = FrameMessageType::Publish;
    frame.payload.assign(data.begin(), data.end());

    EXPECT_TRUE(client.SendFrame(frame));

    for (int i = 0; i < 50 && !received_response.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(received_response.load());
    client.Disconnect();
}
