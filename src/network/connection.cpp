#include "network/connection.h"

#include <unistd.h>
#include <sys/socket.h>
#include <errno.h>
#include <cstring>

#include "protocol/frame_codec.h"

namespace pmqueue {

Connection::Connection(ConnectionId id, int fd)
    : id_(id), fd_(fd) {
    read_buffer_.reserve(4096);
    write_buffer_.reserve(4096);
}

Connection::~Connection() {
    Close();
}

bool Connection::SendFrame(const Frame& frame) {
    if (closed_.load()) {
        return false;
    }

    auto encoded = FrameCodec::Encode(frame);

    std::lock_guard<std::mutex> lock(write_buffer_mutex_);
    write_buffer_.insert(write_buffer_.end(), encoded.begin(), encoded.end());

    // 尝试立即发送
    size_t total_sent = 0;
    while (total_sent < write_buffer_.size()) {
        ssize_t sent = ::send(fd_, write_buffer_.data() + total_sent,
                              write_buffer_.size() - total_sent, MSG_NOSIGNAL);
        if (sent > 0) {
            total_sent += static_cast<size_t>(sent);
        } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            Close();
            return false;
        }
    }

    if (total_sent > 0) {
        write_buffer_.erase(write_buffer_.begin(), write_buffer_.begin() + total_sent);
    }

    return true;
}

void Connection::OnRead() {
    if (closed_.load()) {
        return;
    }

    char temp_buffer[4096];
    while (true) {
        ssize_t n = ::recv(fd_, temp_buffer, sizeof(temp_buffer), 0);
        if (n > 0) {
            std::lock_guard<std::mutex> lock(read_buffer_mutex_);
            read_buffer_.insert(read_buffer_.end(), temp_buffer, temp_buffer + n);
        } else if (n == 0) {
            // 对端关闭
            Close();
            return;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        } else {
            Close();
            return;
        }
    }

    // 解析帧
    while (true) {
        size_t consumed = 0;
        std::optional<Frame> frame;

        {
            std::lock_guard<std::mutex> lock(read_buffer_mutex_);
            frame = FrameCodec::TryDecode(read_buffer_, consumed);
        }

        if (!frame.has_value()) {
            break;
        }

        {
            std::lock_guard<std::mutex> lock(read_buffer_mutex_);
            read_buffer_.erase(read_buffer_.begin(), read_buffer_.begin() + consumed);
        }

        if (frame_handler_) {
            frame_handler_(shared_from_this(), frame.value());
        }
    }
}

void Connection::Close() {
    bool expected = false;
    if (closed_.compare_exchange_strong(expected, true)) {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        if (close_handler_) {
            close_handler_(shared_from_this());
        }
    }
}

} // namespace pmqueue
