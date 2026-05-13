#include "network/tcp_server.h"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>

namespace pmqueue {

TcpServer::TcpServer(uint16_t port) : port_(port) {}

TcpServer::~TcpServer() {
    Stop();
}

bool TcpServer::Start() {
    // 创建监听 socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return false;
    }

    // 设置 SO_REUSEADDR
    int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    // 绑定
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    // 监听
    if (::listen(listen_fd_, 128) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    // 设置非阻塞
    int flags = ::fcntl(listen_fd_, F_GETFL, 0);
    ::fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK);

    // 创建 epoll
    epoll_fd_ = ::epoll_create1(0);
    if (epoll_fd_ < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    // 添加监听 fd 到 epoll
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd_;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);

    running_ = true;
    accept_thread_ = std::thread(&TcpServer::AcceptLoop, this);
    event_thread_ = std::thread(&TcpServer::EventLoop, this);

    return true;
}

void TcpServer::Stop() {
    running_ = false;

    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }

    // 关闭所有连接（先复制列表，避免在持有锁时调用 Close 触发死锁）
    std::vector<Connection::Ptr> conns_to_close;
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (auto& [id, conn] : connections_) {
            conns_to_close.push_back(conn);
        }
        connections_.clear();
    }
    for (auto& conn : conns_to_close) {
        conn->Close();
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    if (event_thread_.joinable()) {
        event_thread_.join();
    }
}

bool TcpServer::SendTo(ConnectionId conn_id, const Frame& frame) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    auto it = connections_.find(conn_id);
    if (it != connections_.end()) {
        return it->second->SendFrame(frame);
    }
    return false;
}

void TcpServer::Broadcast(const Frame& frame) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    for (auto& [id, conn] : connections_) {
        conn->SendFrame(frame);
    }
}

void TcpServer::AcceptLoop() {
    while (running_.load()) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);

        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            break;
        }

        // 设置非阻塞
        int flags = ::fcntl(client_fd, F_GETFL, 0);
        ::fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

        // TCP_NODELAY
        int nodelay = 1;
        ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        ConnectionId conn_id = next_conn_id_.fetch_add(1);
        auto conn = std::make_shared<Connection>(conn_id, client_fd);

        conn->SetFrameHandler([this](const Connection::Ptr& c, const Frame& frame) {
            if (frame_handler_) {
                frame_handler_(c, frame);
            }
        });

        conn->SetCloseHandler([this](const Connection::Ptr& c) {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            connections_.erase(c->GetId());
        });

        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            connections_[conn_id] = conn;
        }

        // 添加到 epoll
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.u64 = conn_id;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);
    }
}

void TcpServer::EventLoop() {
    constexpr int kMaxEvents = 64;
    epoll_event events[kMaxEvents];

    while (running_.load()) {
        int nfds = ::epoll_wait(epoll_fd_, events, kMaxEvents, 100);
        if (nfds < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            ConnectionId conn_id = static_cast<ConnectionId>(events[i].data.u64);

            Connection::Ptr conn;
            {
                std::lock_guard<std::mutex> lock(connections_mutex_);
                auto it = connections_.find(conn_id);
                if (it != connections_.end()) {
                    conn = it->second;
                }
            }
            if (conn) {
                conn->OnRead();
            }
        }
    }
}

} // namespace pmqueue
