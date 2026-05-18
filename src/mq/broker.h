#pragma once

#include <memory>
#include <string>
#include <functional>

#include "common/types.h"
#include "common/config.h"
#include "common/non_copyable.h"
#include "common/rate_limiter.h"
#include "storage/message_store.h"
#include "mq/topic_manager.h"
#include "network/tcp_server.h"
#include "protocol/frame_protocol.h"

namespace pmqueue {

/**
 * @brief 消息队列 Broker
 * 
 * 协调网络层和存储层，处理发布订阅逻辑。
 * 支持两种消费模式：
 * 1. 广播模式：每个订阅者独立消费全部消息
 * 2. 消费者组模式：组内成员轮询分摊消息，每条消息只被组内一个消费者处理
 * 
 * 特性（均通过配置开启/关闭）：
 * - 批量读写
 * - 心跳与连接保活
 * - 背压与限流
 * - 消息 TTL
 * - 消息压缩
 */
class Broker : public NonCopyable {
public:
    explicit Broker(std::unique_ptr<IMessageStore> store, const BrokerConfig& config = BrokerConfig{});
    explicit Broker(std::unique_ptr<IMessageStore> store, uint16_t port);
    ~Broker();

    bool Start();
    void Stop();

    bool IsRunning() const;

    // 发布消息到本地存储（不经过网络）
    bool PublishLocal(const TopicName& topic, const Payload& payload);

    // 获取配置管理器
    ConfigManager& GetConfigManager() { return config_manager_; }

private:
    void OnFrameReceived(const Connection::Ptr& conn, const Frame& frame);
    void HandlePublish(const Connection::Ptr& conn, const Frame& frame);
    void HandleBatchPublish(const Connection::Ptr& conn, const Frame& frame);
    void HandleSubscribe(const Connection::Ptr& conn, const Frame& frame);
    void HandleUnsubscribe(const Connection::Ptr& conn, const Frame& frame);
    void HandlePull(const Connection::Ptr& conn, const Frame& frame);
    void HandleAck(const Connection::Ptr& conn, const Frame& frame);
    void HandlePullDlq(const Connection::Ptr& conn, const Frame& frame);
    void HandlePing(const Connection::Ptr& conn, const Frame& frame);

    void SendResponse(const Connection::Ptr& conn, bool success, const std::string& error_msg = "", MessageId msg_id = 0);
    void SendPushMessage(const Connection::Ptr& conn, const StoredMessage& msg, const std::string& topic, bool compressed);
    void SendBatchPushMessage(const Connection::Ptr& conn, const std::vector<StoredMessage>& messages, const std::string& topic, bool compressed);

    void OnConnectionClosed(ConnectionId conn_id);

    // 根据 Topic 配置和全局配置决定是否压缩
    bool ShouldCompress(const std::string& topic, size_t payload_size) const;

    std::unique_ptr<IMessageStore> store_;
    TopicManager topic_manager_;
    TcpServer server_;
    ConfigManager config_manager_;

    // 全局发布限流
    std::unique_ptr<TokenBucket> global_publish_limiter_;
};

} // namespace pmqueue
