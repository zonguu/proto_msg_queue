#include "mq/broker.h"

#include <chrono>

#include "msg_queue.pb.h"
#include "protocol/frame_codec.h"

namespace pmqueue {

Broker::Broker(std::unique_ptr<IMessageStore> store, uint16_t port)
    : store_(std::move(store)), server_(port) {}

Broker::~Broker() {
    Stop();
}

bool Broker::Start() {
    server_.SetFrameHandler([this](const Connection::Ptr& conn, const Frame& frame) {
        OnFrameReceived(conn, frame);
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

    Payload payload(req.payload().begin(), req.payload().end());
    MessageId msg_id = 0;
    bool success = store_->Publish(req.topic(), payload, msg_id);

    if (success) {
        // 推送给订阅者
        auto subscribers = topic_manager_.GetSubscribers(req.topic());
        if (!subscribers.empty()) {
            pmqueue::PushMessage push_msg;
            push_msg.set_message_id(msg_id);
            push_msg.set_topic(req.topic());
            push_msg.set_payload(req.payload());
            push_msg.set_timestamp(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()));

            std::string push_data;
            push_msg.SerializeToString(&push_data);

            Frame push_frame;
            push_frame.msg_type = FrameMessageType::Push;
            push_frame.payload.assign(push_data.begin(), push_data.end());

            for (const auto& sub : subscribers) {
                server_.SendTo(sub.conn_id, push_frame);
            }
        }
    }

    SendResponse(conn, success, success ? "" : "Publish failed", msg_id);
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

    bool success = topic_manager_.Subscribe(req.topic(), info);
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

    bool success = topic_manager_.Unsubscribe(req.topic(), req.subscriber_id());
    SendResponse(conn, success, success ? "" : "Not subscribed");
}

void Broker::HandlePull(const Connection::Ptr& conn, const Frame& frame) {
    pmqueue::PullRequest req;
    if (!req.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
        SendResponse(conn, false, "Failed to parse pull request");
        return;
    }

    auto messages = store_->Pull(req.topic(), req.subscriber_id(), req.max_messages());

    for (const auto& msg : messages) {
        pmqueue::PushMessage push_msg;
        push_msg.set_message_id(msg.id);
        push_msg.set_topic(msg.topic_name);
        push_msg.set_payload(std::string(msg.payload.begin(), msg.payload.end()));
        push_msg.set_timestamp(msg.timestamp);

        std::string push_data;
        push_msg.SerializeToString(&push_data);

        Frame push_frame;
        push_frame.msg_type = FrameMessageType::Push;
        push_frame.payload.assign(push_data.begin(), push_data.end());
        conn->SendFrame(push_frame);
    }

    SendResponse(conn, true, "", messages.empty() ? 0 : messages.back().id);
}

void Broker::HandleAck(const Connection::Ptr& conn, const Frame& frame) {
    pmqueue::AckRequest req;
    if (!req.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
        SendResponse(conn, false, "Failed to parse ack request");
        return;
    }

    bool success = store_->Ack(req.topic(), req.subscriber_id(), req.message_id());
    SendResponse(conn, success);
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

} // namespace pmqueue
