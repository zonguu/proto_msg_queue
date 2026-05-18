#include "network/tcp_client.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <random>
#include <sstream>
#include <iomanip>

namespace pmqueue {

TcpClient::TcpClient() = default;

TcpClient::~TcpClient() {
    Disconnect();
}

namespace {
std::string GenerateProducerId() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    uint64_t p1 = dis(gen);
    uint64_t p2 = dis(gen);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(8) << (p1 >> 32)
        << std::setw(4) << ((p1 >> 16) & 0xFFFF)
        << std::setw(4) << (p1 & 0xFFFF)
        << std::setw(4) << ((p2 >> 48) & 0xFFFF)
        << std::setw(12) << (p2 & 0xFFFFFFFFFFFF);
    return oss.str();
}
}

bool TcpClient::Connect(const std::string& host, uint16_t port, const BrokerConfig* config) {
    host_ = host;
    port_ = port;
    config_ = config;

    // 生成唯一的生产者标识
    producer_id_ = GenerateProducerId();
    next_sequence_id_.store(1, std::memory_order_relaxed);

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
    auto conn = std::make_shared<Connection>(conn_id, fd, config);
    conn->SetFrameHandler([this](const Connection::Ptr&, const Frame& frame) {
        // 先处理心跳
        if (frame.msg_type == FrameMessageType::Ping) {
            Frame pong;
            pong.msg_type = FrameMessageType::Pong;
            SendFrame(pong);
            return;
        }
        if (frame.msg_type == FrameMessageType::Pong) {
            auto now = std::chrono::steady_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            last_pong_time_ms_.store(static_cast<uint64_t>(ms), std::memory_order_relaxed);
            if (frame_handler_) {
                frame_handler_(frame);
            }
            return;
        }
        if (frame_handler_) {
            frame_handler_(frame);
        }
    });

    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        connection_ = conn;
    }

    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    last_pong_time_ms_.store(static_cast<uint64_t>(ms), std::memory_order_relaxed);

    connected_ = true;
    read_thread_ = std::thread(&TcpClient::ReadLoop, this);

    // 根据配置决定是否启动心跳
    bool heartbeat_enabled = (config_ != nullptr) ? config_->heartbeat_enabled : true;
    if (heartbeat_enabled) {
        ping_thread_ = std::thread(&TcpClient::PingLoop, this);
    }

    return true;
}

void TcpClient::Disconnect() {
    connected_ = false;

    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        if (connection_) {
            connection_->Close();
            connection_.reset();
        }
    }

    if (read_thread_.joinable()) {
        read_thread_.join();
    }
    if (ping_thread_.joinable()) {
        ping_thread_.join();
    }
}

bool TcpClient::SendFrame(const Frame& frame) {
    if (!connected_.load()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(conn_mutex_);
    if (!connection_ || connection_->IsClosed()) {
        return false;
    }
    return connection_->SendFrame(frame);
}

bool TcpClient::IsConnected() const {
    if (!connected_.load()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(conn_mutex_);
    return connection_ && !connection_->IsClosed();
}

void TcpClient::ReadLoop() {
    while (connected_.load()) {
        Connection::Ptr conn;
        {
            std::lock_guard<std::mutex> lock(conn_mutex_);
            conn = connection_;
        }
        if (!conn || conn->IsClosed()) {
            break;
        }
        conn->OnRead();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

uint64_t TcpClient::GetNextSequenceId() {
    return next_sequence_id_.fetch_add(1, std::memory_order_relaxed);
}

void TcpClient::PingLoop() {
    uint32_t interval_ms = (config_ != nullptr) ? config_->heartbeat_interval_ms : 5000;
    uint32_t timeout_ms = (config_ != nullptr) ? config_->heartbeat_timeout_ms : 15000;

    while (connected_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

        if (!connected_.load()) {
            break;
        }

        // 发送 Ping
        Frame ping;
        ping.msg_type = FrameMessageType::Ping;
        if (!SendFrame(ping)) {
            break;
        }

        // 检查是否长时间未收到 Pong
        auto now = std::chrono::steady_clock::now();
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        auto last_pong = static_cast<int64_t>(last_pong_time_ms_.load(std::memory_order_relaxed));
        if (now_ms - last_pong > static_cast<int64_t>(timeout_ms)) {
            // 心跳超时，断开连接
            Connection::Ptr conn;
            {
                std::lock_guard<std::mutex> lock(conn_mutex_);
                conn = connection_;
            }
            if (conn) {
                conn->Close();
            }
            break;
        }
    }
}

} // namespace pmqueue
