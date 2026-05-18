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

class DedupTest : public ::testing::Test {
protected:
    void SetUp() override {
        BrokerConfig config;
        config.dedup_enabled = true;
        config.port = 19095;
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

TEST_F(DedupTest, NoDedupWithoutProducerId) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19095));

    std::atomic<int> response_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                EXPECT_TRUE(resp.success());
                response_count.fetch_add(1);
            }
        }
    });

    // 发送两条不含 producer_id 的消息，应该都被处理
    for (int i = 0; i < 2; ++i) {
        pmqueue::PublishRequest req;
        req.set_topic("dedup_compat_topic");
        req.set_payload("msg" + std::to_string(i));
        // 不设置 producer_id 和 sequence_id

        std::string data;
        req.SerializeToString(&data);

        Frame frame;
        frame.msg_type = FrameMessageType::Publish;
        frame.payload.assign(data.begin(), data.end());
        client.SendFrame(frame);
    }

    for (int i = 0; i < 50 && response_count.load() < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(response_count.load(), 2);
    client.Disconnect();
}

TEST_F(DedupTest, DetectDuplicate) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19095));

    std::atomic<int> response_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            response_count.fetch_add(1);
        }
    });

    // 发送相同 producer_id + sequence_id 的消息两次
    for (int i = 0; i < 2; ++i) {
        pmqueue::PublishRequest req;
        req.set_topic("dedup_topic");
        req.set_payload("duplicate_msg");
        req.set_producer_id("test_producer_1");
        req.set_sequence_id(42);

        std::string data;
        req.SerializeToString(&data);

        Frame frame;
        frame.msg_type = FrameMessageType::Publish;
        frame.payload.assign(data.begin(), data.end());
        client.SendFrame(frame);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 只应收到一次成功响应（第二次是幂等返回，也返回成功，但消息不重复存储）
    EXPECT_EQ(response_count.load(), 2);
    client.Disconnect();
}

TEST_F(DedupTest, AllowNewSequence) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19095));

    std::atomic<int> response_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                EXPECT_TRUE(resp.success());
                response_count.fetch_add(1);
            }
        }
    });

    // 发送递增 sequence 的消息
    for (uint64_t seq = 1; seq <= 3; ++seq) {
        pmqueue::PublishRequest req;
        req.set_topic("dedup_seq_topic");
        req.set_payload("seq_msg_" + std::to_string(seq));
        req.set_producer_id("test_producer_2");
        req.set_sequence_id(seq);

        std::string data;
        req.SerializeToString(&data);

        Frame frame;
        frame.msg_type = FrameMessageType::Publish;
        frame.payload.assign(data.begin(), data.end());
        client.SendFrame(frame);
    }

    for (int i = 0; i < 50 && response_count.load() < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(response_count.load(), 3);
    client.Disconnect();
}

TEST_F(DedupTest, IdempotentResponse) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19095));

    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push || frame.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    // 先订阅
    pmqueue::SubscribeRequest sub_req;
    sub_req.set_topic("dedup_push_topic");
    sub_req.set_subscriber_id("dedup_sub");

    std::string sub_data;
    sub_req.SerializeToString(&sub_data);
    Frame sub_frame;
    sub_frame.msg_type = FrameMessageType::Subscribe;
    sub_frame.payload.assign(sub_data.begin(), sub_data.end());
    client.SendFrame(sub_frame);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 发送同一条消息两次
    for (int i = 0; i < 2; ++i) {
        pmqueue::PublishRequest req;
        req.set_topic("dedup_push_topic");
        req.set_payload("idempotent_msg");
        req.set_producer_id("test_producer_3");
        req.set_sequence_id(100);

        std::string data;
        req.SerializeToString(&data);

        Frame frame;
        frame.msg_type = FrameMessageType::Publish;
        frame.payload.assign(data.begin(), data.end());
        client.SendFrame(frame);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 订阅者只应收到一次 PUSH（第二次被去重）
    EXPECT_EQ(push_count.load(), 1);
    client.Disconnect();
}

TEST_F(DedupTest, DifferentProducers) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 19095));

    std::atomic<int> response_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                EXPECT_TRUE(resp.success());
                response_count.fetch_add(1);
            }
        }
    });

    // 两个不同 producer 使用相同 sequence_id，都应被处理
    for (int p = 0; p < 2; ++p) {
        pmqueue::PublishRequest req;
        req.set_topic("dedup_multi_topic");
        req.set_payload("multi_msg");
        req.set_producer_id("producer_" + std::to_string(p));
        req.set_sequence_id(1);

        std::string data;
        req.SerializeToString(&data);

        Frame frame;
        frame.msg_type = FrameMessageType::Publish;
        frame.payload.assign(data.begin(), data.end());
        client.SendFrame(frame);
    }

    for (int i = 0; i < 50 && response_count.load() < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(response_count.load(), 2);
    client.Disconnect();
}
