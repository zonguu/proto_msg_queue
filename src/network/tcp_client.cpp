#include "network/tcp_client.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>

namespace pmqueue {

TcpClient::TcpClient() = default;

TcpClient::~TcpClient() {
    Disconnect();
}

bool TcpClient::Connect(const std::string& host, uint16_t port) {
    host_ = host;
    port_ = port;

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        ::close(fd);
        return false;
    }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return false;
    }

    // 设置非阻塞
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    // TCP_NODELAY
    int nodelay = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    ConnectionId conn_id = 1;
    connection_ = std::make_shared<Connection>(conn_id, fd);
    connection_->SetFrameHandler([this](const Connection::Ptr&, const Frame& frame) {
        if (frame_handler_) {
            frame_handler_(frame);
        }
    });

    connected_ = true;
    read_thread_ = std::thread(&TcpClient::ReadLoop, this);
    return true;
}

void TcpClient::Disconnect() {
    connected_ = false;
    if (connection_) {
        connection_->Close();
        connection_.reset();
    }
    if (read_thread_.joinable()) {
        read_thread_.join();
    }
}

bool TcpClient::SendFrame(const Frame& frame) {
    if (!connected_.load() || !connection_) {
        return false;
    }
    return connection_->SendFrame(frame);
}

bool TcpClient::IsConnected() const {
    return connected_.load() && connection_ && !connection_->IsClosed();
}

void TcpClient::ReadLoop() {
    while (connected_.load() && connection_ && !connection_->IsClosed()) {
        connection_->OnRead();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

} // namespace pmqueue
