#include "network/connection.h"

#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <errno.h>
#include <cstring>

#include "protocol/frame_codec.h"

namespace pmqueue {

Connection::Connection(ConnectionId id, int fd, const BrokerConfig* config)
    : id_(id), fd_(fd), config_(config) {
    read_buffer_.reserve(4096);
    write_buffer_.reserve(4096);
    UpdateLastActiveTime();
}

Connection::~Connection() {
    Close();
}

bool Connection::SendFrame(const Frame& frame) {
    if (closed_.load()) {
        return false;
    }

    bool use_zero_copy = (config_ != nullptr) ? config_->zero_copy_enabled : true;

    std::lock_guard<std::mutex> lock(write_buffer_mutex_);

    // 如果有待发送的缓冲数据，必须先发送完，避免乱序
    if (!write_buffer_.empty() || !use_zero_copy) {
        auto encoded = FrameCodec::Encode(frame);

        // 写缓冲区背压：超过上限则拒绝写入
        size_t max_write_buffer = (config_ != nullptr) ? config_->max_write_buffer_size : (8 * 1024 * 1024);
        if (write_buffer_.size() + encoded.size() > max_write_buffer) {
            return false;
        }

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

    // === 零拷贝路径：使用 writev 直接发送 header + payload ===
    uint8_t header[kFrameHeaderSize];
    size_t header_size = FrameCodec::EncodeHeaderToBuffer(frame, header, sizeof(header));
    if (header_size == 0) {
        return false;
    }

    struct iovec iov[2];
    iov[0].iov_base = header;
    iov[0].iov_len = header_size;
    iov[1].iov_base = const_cast<uint8_t*>(frame.payload.data());
    iov[1].iov_len = frame.payload.size();

    ssize_t total_to_send = static_cast<ssize_t>(header_size + frame.payload.size());
    ssize_t sent = ::writev(fd_, iov, 2);

    if (sent == total_to_send) {
        // 全部发送成功，零拷贝完成
        return true;
    } else if (sent > 0) {
        // 部分发送，将剩余数据拷贝到 write_buffer_
        size_t remaining_header = (sent < static_cast<ssize_t>(header_size))
            ? (header_size - static_cast<size_t>(sent))
            : 0;
        size_t payload_sent = (sent > static_cast<ssize_t>(header_size))
            ? (static_cast<size_t>(sent) - header_size)
            : 0;
        size_t remaining_payload = frame.payload.size() - payload_sent;

        if (remaining_header > 0) {
            size_t header_offset = header_size - remaining_header;
            write_buffer_.insert(write_buffer_.end(), header + header_offset, header + header_size);
        }
        if (remaining_payload > 0) {
            write_buffer_.insert(write_buffer_.end(),
                frame.payload.begin() + payload_sent,
                frame.payload.end());
        }
        return true;
    } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        // 无法发送任何数据，全部拷贝到 write_buffer_
        write_buffer_.insert(write_buffer_.end(), header, header + header_size);
        write_buffer_.insert(write_buffer_.end(), frame.payload.begin(), frame.payload.end());
        return true;
    } else {
        Close();
        return false;
    }
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

    UpdateLastActiveTime();

    // 使用流式解码器解析帧
    std::lock_guard<std::mutex> lock(read_buffer_mutex_);
    streaming_codec_.AppendData(
        reinterpret_cast<const uint8_t*>(read_buffer_.data()), read_buffer_.size());
    read_buffer_.clear();

    while (true) {
        auto frame = streaming_codec_.TryParseFrame();
        if (!frame.has_value()) {
            break;
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

void Connection::UpdateLastActiveTime() {
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    last_active_time_ms_.store(static_cast<uint64_t>(ms), std::memory_order_relaxed);
}

std::chrono::steady_clock::time_point Connection::GetLastActiveTime() const {
    auto ms = last_active_time_ms_.load(std::memory_order_relaxed);
    return std::chrono::steady_clock::time_point(
        std::chrono::milliseconds(ms));
}

bool Connection::AcquirePublishPermit(uint32_t tokens) {
    // 如果限流被禁用，直接放行
    if (config_ != nullptr && !config_->rate_limit_enabled) {
        return true;
    }

    std::lock_guard<std::mutex> lock(limiter_mutex_);
    if (!publish_limiter_) {
        uint32_t rate = (config_ != nullptr) ? config_->conn_publish_rate : 1000;
        uint32_t burst = (config_ != nullptr) ? config_->rate_limit_burst : 100;
        publish_limiter_ = std::make_unique<TokenBucket>(rate, burst);
    }
    return publish_limiter_->Acquire(tokens);
}

void Connection::SetPublishRateLimit(uint32_t rate_per_second, uint32_t burst_size) {
    std::lock_guard<std::mutex> lock(limiter_mutex_);
    if (!publish_limiter_) {
        publish_limiter_ = std::make_unique<TokenBucket>(rate_per_second, burst_size);
    } else {
        publish_limiter_->SetRate(rate_per_second, burst_size);
    }
}

} // namespace pmqueue
