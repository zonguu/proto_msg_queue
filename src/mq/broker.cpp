#include "mq/broker.h"

#include <chrono>

#include "msg_queue.pb.h"
#include "protocol/frame_codec.h"

namespace pmqueue {

Broker::Broker(std::unique_ptr<IMessageStore> store, uint16_t port)
    : store_(std::move(store))
    , server_(port)
    , global_publish_limiter_(std::make_unique<TokenBucket>(kDefaultGlobalPublishRate, kDefaultRateLimitBurst)) {}

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
    switch (frame.msg_type) {
        case FrameMessageType::Publish:
            HandlePublish(conn, frame);
            break;
        case FrameMessageType::BatchPublish:
            HandleBatchPublish(conn, frame);
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
            HandlePing(conn, frame);
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

    // 单连接限流检查
    if (!conn->AcquirePublishPermit()) {
        SendResponse(conn, false, "Rate limit exceeded");
        return;
    }

    // 全局限流检查
    if (!global_publish_limiter_->Acquire()) {
        SendResponse(conn, false, "Global rate limit exceeded");
        return;
    }

    Payload payload(req.payload().begin(), req.payload().end());
    MessageId msg_id = 0;
    bool success = store_->Publish(req.topic(), payload, msg_id, req.ttl_ms());

    if (!success) {
        SendResponse(conn, false, "Backpressure: buffer full", 0);
        return;
    }

    // 推送给广播订阅者（所有订阅者都收到）
    auto subscribers = topic_manager_.GetSubscribers(req.topic());
    if (!subscribers.empty()) {
        pmqueue::PushMessage push_msg;
        push_msg.set_message_id(msg_id);
        push_msg.set_topic(req.topic());
        push_msg.set_payload(req.payload());
        push_msg.set_timestamp(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()));
        push_msg.set_retry_count(0);

        std::string push_data;
        push_msg.SerializeToString(&push_data);

        Frame push_frame;
        push_frame.msg_type = FrameMessageType::Push;
        push_frame.payload.assign(push_data.begin(), push_data.end());

        for (const auto& sub : subscribers) {
            server_.SendTo(sub.conn_id, push_frame);
        }
    }

    // 消费者组：消息在组内轮询分配，这里只通知有消息到达
    auto groups = topic_manager_.GetConsumerGroups(req.topic());
    for (const auto& group : groups) {
        if (group.members.empty()) {
            continue;
        }
        // Round-Robin 选择一个成员推送
        std::string selected_member = topic_manager_.SelectNextMember(req.topic(), group.group_id);
        if (selected_member.empty()) {
            continue;
        }
        
        // 找到对应成员的 conn_id
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
            push_msg.set_payload(req.payload());
            push_msg.set_timestamp(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()));
            push_msg.set_retry_count(0);

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

    // 批量限流检查
    if (!conn->AcquirePublishPermit(static_cast<uint32_t>(req.messages_size()))) {
        SendResponse(conn, false, "Rate limit exceeded");
        return;
    }

    if (!global_publish_limiter_->Acquire(static_cast<uint32_t>(req.messages_size()))) {
        SendResponse(conn, false, "Global rate limit exceeded");
        return;
    }

    std::vector<StoredMessage> stored_messages;
    stored_messages.reserve(req.messages_size());
    MessageId last_msg_id = 0;
    bool any_success = false;

    for (const auto& msg : req.messages()) {
        Payload payload(msg.payload().begin(), msg.payload().end());
        MessageId msg_id = 0;
        bool success = store_->Publish(msg.topic(), payload, msg_id, msg.ttl_ms());
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
        // 广播订阅者批量推送
        auto subscribers = topic_manager_.GetSubscribers(topic);
        if (!subscribers.empty()) {
            Frame batch_frame;
            batch_frame.msg_type = FrameMessageType::BatchPush;
            pmqueue::BatchPushMessage batch_push;
            for (const auto& msg : messages) {
                auto* push_msg = batch_push.add_messages();
                push_msg->set_message_id(msg.id);
                push_msg->set_topic(topic);
                push_msg->set_payload(std::string(msg.payload.begin(), msg.payload.end()));
                push_msg->set_timestamp(msg.timestamp);
                push_msg->set_retry_count(msg.retry_count);
            }
            std::string batch_data;
            batch_push.SerializeToString(&batch_data);
            batch_frame.payload.assign(batch_data.begin(), batch_data.end());
            for (const auto& sub : subscribers) {
                server_.SendTo(sub.conn_id, batch_frame);
            }
        }

        // 消费者组批量推送
        auto groups = topic_manager_.GetConsumerGroups(topic);
        for (const auto& group : groups) {
            if (group.members.empty()) {
                continue;
            }
            std::string selected_member = topic_manager_.SelectNextMember(topic, group.group_id);
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
                Frame batch_frame;
                batch_frame.msg_type = FrameMessageType::BatchPush;
                pmqueue::BatchPushMessage batch_push;
                for (const auto& msg : messages) {
                    auto* push_msg = batch_push.add_messages();
                    push_msg->set_message_id(msg.id);
                    push_msg->set_topic(topic);
                    push_msg->set_payload(std::string(msg.payload.begin(), msg.payload.end()));
                    push_msg->set_timestamp(msg.timestamp);
                    push_msg->set_retry_count(msg.retry_count);
                }
                std::string batch_data;
                batch_push.SerializeToString(&batch_data);
                batch_frame.payload.assign(batch_data.begin(), batch_data.end());
                server_.SendTo(target_conn_id, batch_frame);
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

    if (messages.size() > 1) {
        // 批量推送
        SendBatchPushMessage(conn, messages, req.topic());
    } else if (messages.size() == 1) {
        SendPushMessage(conn, messages[0], req.topic());
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

void Broker::SendPushMessage(const Connection::Ptr& conn, const StoredMessage& msg, const std::string& topic) {
    pmqueue::PushMessage push_msg;
    push_msg.set_message_id(msg.id);
    push_msg.set_topic(topic);
    push_msg.set_payload(std::string(msg.payload.begin(), msg.payload.end()));
    push_msg.set_timestamp(msg.timestamp);
    push_msg.set_retry_count(msg.retry_count);

    std::string push_data;
    push_msg.SerializeToString(&push_data);

    Frame push_frame;
    push_frame.msg_type = FrameMessageType::Push;
    push_frame.payload.assign(push_data.begin(), push_data.end());
    conn->SendFrame(push_frame);
}

void Broker::SendBatchPushMessage(const Connection::Ptr& conn, const std::vector<StoredMessage>& messages, const std::string& topic) {
    pmqueue::BatchPushMessage batch_push;
    for (const auto& msg : messages) {
        auto* push_msg = batch_push.add_messages();
        push_msg->set_message_id(msg.id);
        push_msg->set_topic(topic);
        push_msg->set_payload(std::string(msg.payload.begin(), msg.payload.end()));
        push_msg->set_timestamp(msg.timestamp);
        push_msg->set_retry_count(msg.retry_count);
    }

    std::string batch_data;
    batch_push.SerializeToString(&batch_data);

    Frame batch_frame;
    batch_frame.msg_type = FrameMessageType::BatchPush;
    batch_frame.payload.assign(batch_data.begin(), batch_data.end());
    conn->SendFrame(batch_frame);
}

void Broker::OnConnectionClosed(ConnectionId conn_id) {
    topic_manager_.UnsubscribeByConnId(conn_id);
}

} // namespace pmqueue
