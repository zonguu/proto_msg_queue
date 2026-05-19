#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

#include "test_util.h"

using namespace pmqueue;

class DedupTest : public TestBrokerFixture {
protected:
    void SetUp() override {
        config_.dedup_enabled = true;
        TestBrokerFixture::SetUp();
    }
};

TEST_F(DedupTest, NoDedupWithoutProducerId) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<int> response_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(),
                                    static_cast<int>(frame.payload.size()))) {
                EXPECT_TRUE(resp.success());
                response_count.fetch_add(1);
            }
        }
    });

    // 发送两条不含 producer_id 的消息，应该都被处理
    for (int i = 0; i < 2; ++i) {
        client.SendFrame(
            BuildPublishFrame("dedup_compat_topic", "msg" + std::to_string(i)));
    }

    EXPECT_TRUE(WaitFor([&]() { return response_count.load() >= 2; }));
    EXPECT_EQ(response_count.load(), 2);
    client.Disconnect();
}

TEST_F(DedupTest, DetectDuplicate) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<int> response_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            response_count.fetch_add(1);
        }
    });

    // 发送相同 producer_id + sequence_id 的消息两次
    for (int i = 0; i < 2; ++i) {
        client.SendFrame(BuildPublishFrame(
            "dedup_topic", "duplicate_msg", "test_producer_1", 42));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 只应收到两次响应（第二次是幂等返回）
    EXPECT_EQ(response_count.load(), 2);
    client.Disconnect();
}

TEST_F(DedupTest, AllowNewSequence) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<int> response_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(),
                                    static_cast<int>(frame.payload.size()))) {
                EXPECT_TRUE(resp.success());
                response_count.fetch_add(1);
            }
        }
    });

    // 发送递增 sequence 的消息
    for (uint64_t seq = 1; seq <= 3; ++seq) {
        client.SendFrame(BuildPublishFrame(
            "dedup_seq_topic", "seq_msg_" + std::to_string(seq),
            "test_producer_2", seq));
    }

    EXPECT_TRUE(WaitFor([&]() { return response_count.load() >= 3; }));
    EXPECT_EQ(response_count.load(), 3);
    client.Disconnect();
}

TEST_F(DedupTest, IdempotentResponse) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<int> push_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Push ||
            frame.msg_type == FrameMessageType::BatchPush) {
            push_count.fetch_add(1);
        }
    });

    // 先订阅
    client.SendFrame(BuildSubscribeFrame("dedup_push_topic", "dedup_sub"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 发送同一条消息两次
    for (int i = 0; i < 2; ++i) {
        client.SendFrame(BuildPublishFrame(
            "dedup_push_topic", "idempotent_msg", "test_producer_3", 100));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 订阅者只应收到一次 PUSH（第二次被去重）
    EXPECT_EQ(push_count.load(), 1);
    client.Disconnect();
}

TEST_F(DedupTest, DifferentProducers) {
    TcpClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", config_.port, &config_));

    std::atomic<int> response_count{0};
    client.SetFrameHandler([&](const Frame& frame) {
        if (frame.msg_type == FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(),
                                    static_cast<int>(frame.payload.size()))) {
                EXPECT_TRUE(resp.success());
                response_count.fetch_add(1);
            }
        }
    });

    // 两个不同 producer 使用相同 sequence_id，都应被处理
    for (int p = 0; p < 2; ++p) {
        client.SendFrame(BuildPublishFrame(
            "dedup_multi_topic", "multi_msg",
            "producer_" + std::to_string(p), 1));
    }

    EXPECT_TRUE(WaitFor([&]() { return response_count.load() >= 2; }));
    EXPECT_EQ(response_count.load(), 2);
    client.Disconnect();
}
