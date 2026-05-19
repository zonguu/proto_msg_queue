#include "test_util.h"

namespace pmqueue {

// ---------------------------------------------------------------------------
// 端口分配（线程安全）
// ---------------------------------------------------------------------------
static std::atomic<uint16_t> g_next_test_port{20000};

uint16_t AllocateTestPort() {
    return g_next_test_port.fetch_add(1, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// 通用等待
// ---------------------------------------------------------------------------
bool WaitFor(std::function<bool()> condition, int timeout_ms, int poll_ms) {
    const int iterations = timeout_ms / poll_ms;
    for (int i = 0; i < iterations; ++i) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
    }
    return condition();
}

// ---------------------------------------------------------------------------
// 帧构建辅助函数
// ---------------------------------------------------------------------------
Frame BuildPublishFrame(const std::string& topic,
                        const std::string& payload,
                        uint32_t ttl_ms) {
    pmqueue::PublishRequest req;
    req.set_topic(topic);
    req.set_payload(payload);
    if (ttl_ms > 0) {
        req.set_ttl_ms(ttl_ms);
    }
    return BuildFrame(FrameMessageType::Publish, req);
}

Frame BuildPublishFrame(const std::string& topic,
                        const std::string& payload,
                        const std::string& producer_id,
                        uint64_t sequence_id,
                        uint32_t ttl_ms) {
    pmqueue::PublishRequest req;
    req.set_topic(topic);
    req.set_payload(payload);
    req.set_producer_id(producer_id);
    req.set_sequence_id(sequence_id);
    if (ttl_ms > 0) {
        req.set_ttl_ms(ttl_ms);
    }
    return BuildFrame(FrameMessageType::Publish, req);
}

Frame BuildSubscribeFrame(const std::string& topic,
                          const std::string& sub_id,
                          const std::string& group_id) {
    pmqueue::SubscribeRequest req;
    req.set_topic(topic);
    req.set_subscriber_id(sub_id);
    if (!group_id.empty()) {
        req.set_group_id(group_id);
    }
    return BuildFrame(FrameMessageType::Subscribe, req);
}

Frame BuildUnsubscribeFrame(const std::string& topic,
                            const std::string& sub_id,
                            const std::string& group_id) {
    pmqueue::UnsubscribeRequest req;
    req.set_topic(topic);
    req.set_subscriber_id(sub_id);
    if (!group_id.empty()) {
        req.set_group_id(group_id);
    }
    return BuildFrame(FrameMessageType::Unsubscribe, req);
}

Frame BuildPullFrame(const std::string& topic,
                     const std::string& sub_id,
                     const std::string& group_id,
                     uint32_t max_messages) {
    pmqueue::PullRequest req;
    req.set_topic(topic);
    req.set_subscriber_id(sub_id);
    req.set_max_messages(max_messages);
    if (!group_id.empty()) {
        req.set_group_id(group_id);
    }
    return BuildFrame(FrameMessageType::Pull, req);
}

Frame BuildAckFrame(const std::string& topic,
                    const std::string& sub_id,
                    MessageId msg_id,
                    const std::string& group_id) {
    pmqueue::AckRequest req;
    req.set_topic(topic);
    req.set_subscriber_id(sub_id);
    req.set_message_id(msg_id);
    if (!group_id.empty()) {
        req.set_group_id(group_id);
    }
    return BuildFrame(FrameMessageType::Ack, req);
}

Frame BuildBatchPublishFrame(
    const std::vector<std::pair<std::string, std::string>>& topic_payloads) {
    pmqueue::BatchPublishRequest batch_req;
    for (const auto& tp : topic_payloads) {
        auto* msg = batch_req.add_messages();
        msg->set_topic(tp.first);
        msg->set_payload(tp.second);
    }
    return BuildFrame(FrameMessageType::BatchPublish, batch_req);
}

Frame BuildAdminFrame(pmqueue::AdminCommandType cmd,
                      const std::string& topic) {
    pmqueue::AdminRequest req;
    req.set_command(cmd);
    if (!topic.empty()) {
        req.set_topic(topic);
    }
    return BuildFrame(FrameMessageType::Admin, req);
}

// ---------------------------------------------------------------------------
// 发送并等待 Response
// ---------------------------------------------------------------------------
std::optional<pmqueue::Response> SendAndWaitResponse(TcpClient& client,
                                                     const Frame& frame,
                                                     int timeout_ms) {
    pmqueue::Response resp;
    std::atomic<bool> received{false};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Response) {
            if (resp.ParseFromArray(f.payload.data(),
                                    static_cast<int>(f.payload.size()))) {
                received.store(true);
            }
        }
    });

    if (!client.SendFrame(frame)) {
        return std::nullopt;
    }

    if (!WaitFor([&]() { return received.load(); }, timeout_ms)) {
        return std::nullopt;
    }
    return resp;
}

// ---------------------------------------------------------------------------
// 发送 Admin 命令并等待响应
// ---------------------------------------------------------------------------
std::optional<pmqueue::AdminResponse> SendAdminCommand(
    TcpClient& client,
    pmqueue::AdminCommandType cmd,
    const std::string& topic) {
    pmqueue::AdminResponse resp;
    std::atomic<bool> received{false};
    client.SetFrameHandler([&](const Frame& f) {
        if (f.msg_type == FrameMessageType::Admin) {
            if (resp.ParseFromArray(f.payload.data(),
                                    static_cast<int>(f.payload.size()))) {
                received.store(true);
            }
        }
    });

    auto frame = BuildAdminFrame(cmd, topic);
    if (!client.SendFrame(frame)) {
        return std::nullopt;
    }

    if (!WaitFor([&]() { return received.load(); }, 2000, 50)) {
        return std::nullopt;
    }
    return resp;
}

} // namespace pmqueue
