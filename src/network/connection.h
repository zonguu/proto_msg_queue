#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/non_copyable.h"
#include "protocol/frame_protocol.h"

namespace pmqueue {

class Connection : public NonCopyable, public std::enable_shared_from_this<Connection> {
public:
    using Ptr = std::shared_ptr<Connection>;
    using FrameHandler = std::function<void(const Ptr&, const Frame&)>;
    using CloseHandler = std::function<void(const Ptr&)>;

    Connection(ConnectionId id, int fd);
    ~Connection();

    ConnectionId GetId() const { return id_; }
    int GetFd() const { return fd_; }

    void SetFrameHandler(FrameHandler handler) { frame_handler_ = std::move(handler); }
    void SetCloseHandler(CloseHandler handler) { close_handler_ = std::move(handler); }

    // 发送帧
    bool SendFrame(const Frame& frame);

    // 读取数据（由网络层调用）
    void OnRead();

    // 关闭连接
    void Close();

    bool IsClosed() const { return closed_.load(); }

private:
    ConnectionId id_;
    int fd_;
    std::atomic<bool> closed_{false};

    std::mutex read_buffer_mutex_;
    std::vector<uint8_t> read_buffer_;

    std::mutex write_buffer_mutex_;
    std::vector<uint8_t> write_buffer_;

    FrameHandler frame_handler_;
    CloseHandler close_handler_;
};

} // namespace pmqueue
