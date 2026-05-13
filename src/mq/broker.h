#pragma once

#include <memory>
#include <string>
#include <functional>

#include "common/types.h"
#include "common/non_copyable.h"
#include "storage/message_store.h"
#include "mq/topic_manager.h"
#include "network/tcp_server.h"
#include "protocol/frame_protocol.h"

namespace pmqueue {

/**
 * @brief 消息队列 Broker
 * 
 * 协调网络层和存储层，处理发布订阅逻辑。
 */
class Broker : public NonCopyable {
public:
    explicit Broker(std::unique_ptr<IMessageStore> store, uint16_t port = 9090);
    ~Broker();

    bool Start();
    void Stop();

    bool IsRunning() const;

    // 发布消息到本地存储（不经过网络）
    bool PublishLocal(const TopicName& topic, const Payload& payload);

private:
    void OnFrameReceived(const Connection::Ptr& conn, const Frame& frame);
    void HandlePublish(const Connection::Ptr& conn, const Frame& frame);
    void HandleSubscribe(const Connection::Ptr& conn, const Frame& frame);
    void HandleUnsubscribe(const Connection::Ptr& conn, const Frame& frame);
    void HandlePull(const Connection::Ptr& conn, const Frame& frame);
    void HandleAck(const Connection::Ptr& conn, const Frame& frame);

    void SendResponse(const Connection::Ptr& conn, bool success, const std::string& error_msg = "", MessageId msg_id = 0);

    std::unique_ptr<IMessageStore> store_;
    TopicManager topic_manager_;
    TcpServer server_;
};

} // namespace pmqueue
