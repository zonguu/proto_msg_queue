#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "common/non_copyable.h"
#include "network/connection.h"

namespace pmqueue {

class TcpServer : public NonCopyable {
public:
    using FrameHandler = std::function<void(const Connection::Ptr&, const Frame&)>;

    explicit TcpServer(uint16_t port);
    ~TcpServer();

    bool Start();
    void Stop();

    void SetFrameHandler(FrameHandler handler) { frame_handler_ = std::move(handler); }

    // 发送帧到指定连接
    bool SendTo(ConnectionId conn_id, const Frame& frame);

    // 广播帧到所有连接
    void Broadcast(const Frame& frame);

    bool IsRunning() const { return running_.load(); }

private:
    void AcceptLoop();
    void EventLoop();

    uint16_t port_;
    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    std::atomic<bool> running_{false};

    std::thread accept_thread_;
    std::thread event_thread_;

    std::mutex connections_mutex_;
    std::unordered_map<ConnectionId, Connection::Ptr> connections_;
    std::atomic<ConnectionId> next_conn_id_{1};

    FrameHandler frame_handler_;
};

} // namespace pmqueue
