#pragma once

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "mq/broker.h"
#include "network/tcp_client.h"
#include "storage/memory_message_store.h"
#include "protocol/frame_protocol.h"
#include "msg_queue.pb.h"

namespace pmqueue {

// ---------------------------------------------------------------------------
// 端口分配
// ---------------------------------------------------------------------------
uint16_t AllocateTestPort();

// ---------------------------------------------------------------------------
// 测试 Broker 夹具（默认禁用心跳以加速测试）
// ---------------------------------------------------------------------------
class TestBrokerFixture : public ::testing::Test {
protected:
    BrokerConfig config_;
    std::unique_ptr<Broker> broker_;

    void SetUp() override {
        ApplyDefaultTestConfig(config_);
        auto store = CreateStore();
        broker_ = std::make_unique<Broker>(std::move(store), config_);
        ASSERT_TRUE(broker_->Start());
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    void TearDown() override {
        if (broker_) {
            broker_->Stop();
        }
    }

    // 子类可覆盖以自定义 Store 参数
    virtual std::unique_ptr<IMessageStore> CreateStore() {
        return std::make_unique<MemoryMessageStore>(1024 * 1024);
    }

    static void ApplyDefaultTestConfig(BrokerConfig& cfg) {
        cfg.port = AllocateTestPort();
        cfg.heartbeat_enabled = false;
    }
};

// ---------------------------------------------------------------------------
// 通用等待
// ---------------------------------------------------------------------------
bool WaitFor(std::function<bool()> condition,
             int timeout_ms = 2000,
             int poll_ms = 50);

// ---------------------------------------------------------------------------
// 帧构建辅助函数
// ---------------------------------------------------------------------------
template <typename T>
Frame BuildFrame(FrameMessageType msg_type, const T& proto) {
    std::string data;
    proto.SerializeToString(&data);
    Frame frame;
    frame.msg_type = msg_type;
    frame.payload.assign(data.begin(), data.end());
    return frame;
}

Frame BuildPublishFrame(const std::string& topic,
                        const std::string& payload,
                        uint32_t ttl_ms = 0);

Frame BuildPublishFrame(const std::string& topic,
                        const std::string& payload,
                        const std::string& producer_id,
                        uint64_t sequence_id,
                        uint32_t ttl_ms = 0);

Frame BuildSubscribeFrame(const std::string& topic,
                          const std::string& sub_id,
                          const std::string& group_id = "");

Frame BuildUnsubscribeFrame(const std::string& topic,
                            const std::string& sub_id,
                            const std::string& group_id = "");

Frame BuildPullFrame(const std::string& topic,
                     const std::string& sub_id,
                     const std::string& group_id = "",
                     uint32_t max_messages = 10);

Frame BuildAckFrame(const std::string& topic,
                    const std::string& sub_id,
                    MessageId msg_id,
                    const std::string& group_id = "");

Frame BuildBatchPublishFrame(
    const std::vector<std::pair<std::string, std::string>>& topic_payloads);

Frame BuildAdminFrame(pmqueue::AdminCommandType cmd,
                      const std::string& topic = "");

// ---------------------------------------------------------------------------
// 发送并等待 Response / AdminResponse
// ---------------------------------------------------------------------------
std::optional<pmqueue::Response> SendAndWaitResponse(
    TcpClient& client,
    const Frame& frame,
    int timeout_ms = 2000);

std::optional<pmqueue::AdminResponse> SendAdminCommand(
    TcpClient& client,
    pmqueue::AdminCommandType cmd,
    const std::string& topic = "");

} // namespace pmqueue
