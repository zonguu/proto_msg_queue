#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/types.h"
#include "common/non_copyable.h"
#include "network/connection.h"

namespace pmqueue {

class TcpClient : public NonCopyable {
public:
    using FrameHandler = std::function<void(const Frame&)>;

    TcpClient();
    ~TcpClient();

    bool Connect(const std::string& host, uint16_t port);
    void Disconnect();

    bool SendFrame(const Frame& frame);
    bool IsConnected() const;

    void SetFrameHandler(FrameHandler handler) { frame_handler_ = std::move(handler); }

private:
    void ReadLoop();

    std::string host_;
    uint16_t port_ = 0;
    Connection::Ptr connection_;
    std::atomic<bool> connected_{false};
    std::thread read_thread_;
    FrameHandler frame_handler_;
};

} // namespace pmqueue
