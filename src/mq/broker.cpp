#include "mq/broker.h"

#include <chrono>

#include "msg_queue.pb.h"
#include "protocol/frame_codec.h"
#include "common/compression.h"

namespace pmqueue {

Broker::Broker(std::unique_ptr<IMessageStore> store, const BrokerConfig& config)
    : store_(std::move(store))
    , server_(config)
    , config_manager_(config)
    , global_publish_limiter_(config.rate_limit_enabled
        ? std::make_unique<TokenBucket>(config.global_publish_rate, config.rate_limit_burst)
        : nullptr) {}

Broker::Broker(std::unique_ptr<IMessageStore> store, uint16_t port)
    : Broker(std::move(store), [port]() {
        BrokerConfig cfg;
        cfg.port = port;
        return cfg;
    }()) {}

Broker::~Broker() {
    Stop();
}

bool Broker::Start() {
    server_.SetFrameHandler([this](const Connection::Ptr& conn, const Frame& frame) {
        OnFrameReceived(conn, frame);
    });
    server_.SetIdleTimeoutHandler([this](ConnectionId conn_id) {
        OnConnectionClosed(conn_id);
    });
    return server_.Start();
}

void Broker::Stop() {
    server_.Stop();
}

bool Broker::IsRunning() const {
    return server_.IsRunning();
}

bool Broker::PublishLocal(const TopicName& topic, const Payload& payload) {
    MessageId msg_id = 0;
    return store_->Publish(topic, payload, msg_id);
}

void Broker::OnFrameReceived(const Connection::Ptr& conn, const Frame& frame) {
    const auto& config = config_manager_.GetGlobalConfig();

    switch (frame.msg_type) {
        case FrameMessageType::Publish:
            HandlePublish(conn, frame);
            break;
        case FrameMessageType::BatchPublish:
            if (config.batch_publish_enabled) {
                HandleBatchPublish(conn, frame);
            } else {
                SendResponse(conn, false, "Batch publish is disabled");
            }
            break;
        case FrameMessageType::Subscribe:
            HandleSubscribe(conn, frame);
            break;
        case FrameMessageType::Unsubscribe:
            HandleUnsubscribe(conn, frame);
            break;
        case FrameMessageType::Pull:
            HandlePull(conn, frame);
            break;
        case FrameMessageType::Ack:
            HandleAck(conn, frame);
            break;
        case FrameMessageType::Ping:
            if (config.heartbeat_enabled) {
                HandlePing(conn, frame);
            }
            break;
        case FrameMessageType::Unknown:
        default:
            SendResponse(conn, false, "Unknown message type");
            break;
    }
}

void Broker::HandlePublish(const Connection::Ptr& conn, const Frame& frame) {
    pmqueue::PublishRequest req;
    if (!req.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
        SendResponse(conn, false, "Failed to parse publish request");
        return;
    }

    const auto& config = config_manager_.GetGlobalConfig();

    // 单连接限流检查
    if (config.rate_limit_enabled && !conn->AcquirePublishPermit()) {
        SendResponse(conn, false, "Rate limit exceeded");
        return;
    }

    // 全局限流检查
    if (config.rate_limit_enabled && global_publish_limiter_ && !global_publish_limiter_->Acquire()) {
        SendResponse(conn, false, "Global rate limit exceeded");
        return;
    }

    // 处理压缩：客户端可能已压缩，或 Broker 根据配置决定是否压缩
    Payload payload(req.payload().begin(), req.payload().end());
    bool is_compressed = req.compressed();

    // 如果 Topic/全局配置要求压缩且客户端未压缩，Broker 端压缩
    bool should_compress = ShouldCompress(req.topic(), payload.size());
    if (should_compress && !is_compressed) {
        auto compressed = Compress(payload);
        if (!compressed.empty() && compressed.size() < payload.size()) {
            payload = std::move(compressed);
            is_compressed = true;
        }
    }

    uint32_t ttl_ms = config.ttl_enabled ? req.ttl_ms() : 0;

    MessageId msg_id = 0;
    bool success = store_->Publish(req.topic(), payload, msg_id, ttl_ms);

    if (!success) {
        SendResponse(conn, false, "Backpressure: buffer full", 0);
        return;
    }

    // 推送给广播订阅者
    auto subscribers = topic_manager_.GetSubscribers(req.topic());
    if (!subscribers.empty()) {
        pmqueue::PushMessage push_msg;
        push_msg.set_message_id(msg_id);
        push_msg.set_topic(req.topic());
        push_msg.set_payload(std::string(payload.begin(), payload.end()));
        push_msg.set_timestamp(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()));
        push_msg.set_retry_count(0);
        push_msg.set_compressed(is_compressed);

        std::string push_data;
        push_msg.SerializeToString(&push_data);

        Frame push_frame;
        push_frame.msg_type = FrameMessageType::Push;
        push_frame.payload.assign(push_data.begin(), push_data.end());

        for (const auto& sub : subscribers) {
            server_.SendTo(sub.conn_id, push_frame);
        }
    }

    // 消费者组推送
    auto groups = topic_manager_.GetConsumerGroups(req.topic());
    for (const auto& group : groups) {
        if (group.members.empty()) {
            continue;
        }
        std::string selected_member = topic_manager_.SelectNextMember(req.topic(), group.group_id);
        if (selected_member.empty()) {
            continue;
        }
        
        ConnectionId target_conn_id = 0;
        for (const auto& member : group.members) {
            if (member.subscriber_id == selected_member) {
                target_conn_id = member.conn_id;
                break;
            }
        }
        
        if (target_conn_id != 0) {
            pmqueue::PushMessage push_msg;
            push_msg.set_message_id(msg_id);
            push_msg.set_topic(req.topic());
            push_msg.set_payload(std::string(payload.begin(), payload.end()));
            push_msg.set_timestamp(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()));
            push_msg.set_retry_count(0);
            push_msg.set_compressed(is_compressed);

            std::string push_data;
            push_msg.SerializeToString(&push_data);

            Frame push_frame;
            push_frame.msg_type = FrameMessageType::Push;
            push_frame.payload.assign(push_data.begin(), push_data.end());
            server_.SendTo(target_conn_id, push_frame);
        }
    }

    SendResponse(conn, true, "", msg_id);
}

void Broker::HandleBatchPublish(const Connection::Ptr& conn, const Frame& frame) {
    pmqueue::BatchPublishRequest req;
    if (!req.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
        SendResponse(conn, false, "Failed to parse batch publish request");
        return;
    }

    if (req.messages_size() == 0) {
        SendResponse(conn, false, "Empty batch publish request");
        return;
    }

    const auto& config = config_manager_.GetGlobalConfig();

    // 批量限流检查
    if (config.rate_limit_enabled && !conn->AcquirePublishPermit(static_cast<uint32_t>(req.messages_size()))) {
        SendResponse(conn, false, "Rate limit exceeded");
        return;
    }

    if (config.rate_limit_enabled && global_publish_limiter_ &&
        !global_publish_limiter_->Acquire(static_cast<uint32_t>(req.messages_size()))) {
        SendResponse(conn, false, "Global rate limit exceeded");
        return;
    }

    // 检查批量大小限制
    if (static_cast<uint32_t>(req.messages_size()) > config.max_batch_size) {
        SendResponse(conn, false, "Batch size exceeds limit");
        return;
    }

    std::vector<StoredMessage> stored_messages;
    stored_messages.reserve(req.messages_size());
    MessageId last_msg_id = 0;
    bool any_success = false;

    for (const auto& msg : req.messages()) {
        Payload payload(msg.payload().begin(), msg.payload().end());
        bool is_compressed = msg.compressed();
        bool should_compress = ShouldCompress(msg.topic(), payload.size());
        if (should_compress && !is_compressed) {
            auto compressed = Compress(payload);
            if (!compressed.empty() && compressed.size() < payload.size()) {
                payload = std::move(compressed);
                is_compressed = true;
            }
        }

        uint32_t ttl_ms = config.ttl_enabled ? msg.ttl_ms() : 0;
        MessageId msg_id = 0;
        bool success = store_->Publish(msg.topic(), payload, msg_id, ttl_ms);
        if (success) {
            any_success = true;
            last_msg_id = msg_id;
            StoredMessage stored;
            stored.id = msg_id;
            stored.topic_name = msg.topic();
            stored.payload = payload;
            stored.timestamp = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
            stored.retry_count = 0;
            stored_messages.push_back(std::move(stored));
        }
    }

    if (!any_success) {
        SendResponse(conn, false, "Backpressure: buffer full", 0);
        return;
    }

    // 按 topic 分组推送
    std::unordered_map<std::string, std::vector<StoredMessage>> topic_messages;
    for (auto& msg : stored_messages) {
        topic_messages[msg.topic_name].push_back(std::move(msg));
    }

    for (auto& [topic, messages] : topic_messages) {
        bool use_batch = config.batch_push_enabled && messages.size() > 1;

        // 广播订阅者
        auto subscribers = topic_manager_.GetSubscribers(topic);
        if (!subscribers.empty()) {
            if (use_batch) {
                SendBatchPushMessage(nullptr, messages, topic, false);
                Frame batch_frame;
                pmqueue::BatchPushMessage batch_push;
                for (const auto& msg : messages) {
                    auto* push_msg = batch_push.add_messages();
                    push_msg->set_message_id(msg.id);
                    push_msg->set_topic(topic);
                    push_msg->set_payload(std::string(msg.payload.begin(), msg.payload.end()));
                    push_msg->set_timestamp(msg.timestamp);
                    push_msg->set_retry_count(msg.retry_count);
                    push_msg->set_compressed(false);
                }
                std::string batch_data;
                batch_push.SerializeToString(&batch_data);
                batch_frame.msg_type = FrameMessageType::BatchPush;
                batch_frame.payload.assign(batch_data.begin(), batch_data.end());
                for (const auto& sub : subscribers) {
                    server_.SendTo(sub.conn_id, batch_frame);
                }
            } else {
                for (const auto& sub : subscribers) {
                    for (const auto& msg : messages) {
                        SendPushMessage(nullptr, msg, topic, false);
                    }
                    Frame push_frame;
                    pmqueue::PushMessage push_msg;
                    push_msg.set_message_id(messages[0].id);
                    push_msg.set_topic(topic);
                    push_msg.set_payload(std::string(messages[0].payload.begin(), messages[0].payload.end()));
                    push_msg.set_timestamp(messages[0].timestamp);
                    push_msg.set_retry_count(messages[0].retry_count);
                    push_msg.set_compressed(false);
                    std::string push_data;
                    push_msg.SerializeToString(&push_data);
                    push_frame.msg_type = FrameMessageType::Push;
                    push_frame.payload.assign(push_data.begin(), push_data.end());
                    server_.SendTo(sub.conn_id, push_frame);
                }
            }
        }

        // 消费者组
        auto groups = topic_manager_.GetConsumerGroups(topic);
        for (const auto& group : groups) {
            if (group.members.empty()) continue;
            std::string selected_member = topic_manager_.SelectNextMember(topic, group.group_id);
            if (selected_member.empty()) continue;
            ConnectionId target_conn_id = 0;
            for (const auto& member : group.members) {
                if (member.subscriber_id == selected_member) {
                    target_conn_id = member.conn_id;
                    break;
                }
            }
            if (target_conn_id != 0) {
                if (use_batch) {
                    Frame batch_frame;
                    pmqueue::BatchPushMessage batch_push;
                    for (const auto& msg : messages) {
                        auto* push_msg = batch_push.add_messages();
                        push_msg->set_message_id(msg.id);
                        push_msg->set_topic(topic);
                        push_msg->set_payload(std::string(msg.payload.begin(), msg.payload.end()));
                        push_msg->set_timestamp(msg.timestamp);
                        push_msg->set_retry_count(msg.retry_count);
                        push_msg->set_compressed(false);
                    }
                    std::string batch_data;
                    batch_push.SerializeToString(&batch_data);
                    batch_frame.msg_type = FrameMessageType::BatchPush;
                    batch_frame.payload.assign(batch_data.begin(), batch_data.end());
                    server_.SendTo(target_conn_id, batch_frame);
                } else {
                    Frame push_frame;
                    pmqueue::PushMessage push_msg;
                    push_msg.set_message_id(messages[0].id);
                    push_msg.set_topic(topic);
                    push_msg.set_payload(std::string(messages[0].payload.begin(), messages[0].payload.end()));
                    push_msg.set_timestamp(messages[0].timestamp);
                    push_msg.set_retry_count(messages[0].retry_count);
                    push_msg.set_compressed(false);
                    std::string push_data;
                    push_msg.SerializeToString(&push_data);
                    push_frame.msg_type = FrameMessageType::Push;
                    push_frame.payload.assign(push_data.begin(), push_data.end());
                    server_.SendTo(target_conn_id, push_frame);
                }
            }
        }
    }

    SendResponse(conn, true, "", last_msg_id);
}

void Broker::HandleSubscribe(const Connection::Ptr& conn, const Frame& frame) {
    pmqueue::SubscribeRequest req;
    if (!req.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
        SendResponse(conn, false, "Failed to parse subscribe request");
        return;
    }

    SubscriberInfo info;
    info.id = req.subscriber_id();
    info.conn_id = conn->GetId();

    bool success = topic_manager_.Subscribe(req.topic(), info, req.group_id());
    if (success) {
        store_->CreateTopic(req.topic());
    }

    SendResponse(conn, success, success ? "" : "Already subscribed");
}

void Broker::HandleUnsubscribe(const Connection::Ptr& conn, const Frame& frame) {
    pmqueue::UnsubscribeRequest req;
    if (!req.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
        SendResponse(conn, false, "Failed to parse unsubscribe request");
        return;
    }

    bool success = topic_manager_.Unsubscribe(req.topic(), req.subscriber_id(), req.group_id());
    SendResponse(conn, success, success ? "" : "Not subscribed");
}

void Broker::HandlePull(const Connection::Ptr& conn, const Frame& frame) {
    pmqueue::PullRequest req;
    if (!req.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
        SendResponse(conn, false, "Failed to parse pull request");
        return;
    }

    const bool is_group = !req.group_id().empty();
    const std::string& consumer_id = is_group ? req.group_id() : req.subscriber_id();

    auto messages = store_->Pull(req.topic(), consumer_id, req.max_messages(), is_group);

    const auto& config = config_manager_.GetGlobalConfig();
    bool use_batch = config.batch_push_enabled && messages.size() > 1;

    if (use_batch) {
        SendBatchPushMessage(conn, messages, req.topic(), false);
    } else {
        for (const auto& msg : messages) {
            SendPushMessage(conn, msg, req.topic(), false);
        }
    }

    SendResponse(conn, true, "", messages.empty() ? 0 : messages.back().id);
}

void Broker::HandleAck(const Connection::Ptr& conn, const Frame& frame) {
    pmqueue::AckRequest req;
    if (!req.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
        SendResponse(conn, false, "Failed to parse ack request");
        return;
    }

    const bool is_group = !req.group_id().empty();
    const std::string& consumer_id = is_group ? req.group_id() : req.subscriber_id();

    bool success = store_->Ack(req.topic(), consumer_id, req.message_id(), is_group);
    SendResponse(conn, success);
}

void Broker::HandlePing(const Connection::Ptr& conn, const Frame& /*frame*/) {
    Frame pong;
    pong.msg_type = FrameMessageType::Pong;
    conn->SendFrame(pong);
}

void Broker::SendResponse(const Connection::Ptr& conn, bool success, const std::string& error_msg, MessageId msg_id) {
    pmqueue::Response resp;
    resp.set_success(success);
    resp.set_error_msg(error_msg);
    resp.set_message_id(msg_id);

    std::string resp_data;
    resp.SerializeToString(&resp_data);

    Frame frame;
    frame.msg_type = FrameMessageType::Response;
    frame.payload.assign(resp_data.begin(), resp_data.end());
    conn->SendFrame(frame);
}

void Broker::SendPushMessage(const Connection::Ptr& conn, const StoredMessage& msg, const std::string& topic, bool compressed) {
    (void)conn; // conn may be null when called for pre-serialization
    pmqueue::PushMessage push_msg;
    push_msg.set_message_id(msg.id);
    push_msg.set_topic(topic);
    push_msg.set_payload(std::string(msg.payload.begin(), msg.payload.end()));
    push_msg.set_timestamp(msg.timestamp);
    push_msg.set_retry_count(msg.retry_count);
    push_msg.set_compressed(compressed);

    std::string push_data;
    push_msg.SerializeToString(&push_data);

    Frame push_frame;
    push_frame.msg_type = FrameMessageType::Push;
    push_frame.payload.assign(push_data.begin(), push_data.end());
    if (conn) {
        conn->SendFrame(push_frame);
    }
}

void Broker::SendBatchPushMessage(const Connection::Ptr& conn, const std::vector<StoredMessage>& messages, const std::string& topic, bool compressed) {
    (void)conn;
    pmqueue::BatchPushMessage batch_push;
    for (const auto& msg : messages) {
        auto* push_msg = batch_push.add_messages();
        push_msg->set_message_id(msg.id);
        push_msg->set_topic(topic);
        push_msg->set_payload(std::string(msg.payload.begin(), msg.payload.end()));
        push_msg->set_timestamp(msg.timestamp);
        push_msg->set_retry_count(msg.retry_count);
        push_msg->set_compressed(compressed);
    }

    std::string batch_data;
    batch_push.SerializeToString(&batch_data);

    Frame batch_frame;
    batch_frame.msg_type = FrameMessageType::BatchPush;
    batch_frame.payload.assign(batch_data.begin(), batch_data.end());
    if (conn) {
        conn->SendFrame(batch_frame);
    }
}

void Broker::OnConnectionClosed(ConnectionId conn_id) {
    topic_manager_.UnsubscribeByConnId(conn_id);
}

bool Broker::ShouldCompress(const std::string& topic, size_t payload_size) const {
    const auto& global_config = config_manager_.GetGlobalConfig();
    if (!global_config.compression_enabled) {
        return false;
    }
    if (payload_size < global_config.compression_threshold_bytes) {
        return false;
    }
    // Topic 级配置可覆盖全局
    auto topic_config = topic_manager_.GetTopicConfig(topic);
    if (topic_config.compression_enabled) {
        return true;
    }
    // 如果 Topic 没有单独配置，使用全局配置
    return global_config.compression_enabled;
}

} // namespace pmqueue
