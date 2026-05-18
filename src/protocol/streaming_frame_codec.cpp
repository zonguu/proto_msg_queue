#include "protocol/streaming_frame_codec.h"

#include <cstring>

#include "common/types.h"

namespace pmqueue {

void StreamingFrameCodec::AppendData(const uint8_t* data, size_t len) {
    if (data && len > 0) {
        buffer_.insert(buffer_.end(), data, data + len);
    }
}

std::optional<Frame> StreamingFrameCodec::TryParseFrame() {
    while (true) {
        switch (state_) {
            case ParseState::WaitingMagic: {
                if (!SyncMagic()) {
                    return std::nullopt;
                }
                state_ = ParseState::WaitingHeader;
                break;
            }

            case ParseState::WaitingHeader: {
                if (!ParseHeader()) {
                    // 如果 ParseHeader 将状态改回 WaitingMagic，说明遇到了损坏数据，
                    // 继续尝试解析下一个帧；否则是数据不足
                    if (state_ == ParseState::WaitingMagic) {
                        break;
                    }
                    return std::nullopt;
                }
                state_ = ParseState::WaitingPayload;
                break;
            }

            case ParseState::WaitingPayload: {
                Frame frame;
                if (!ParsePayload(frame)) {
                    return std::nullopt;
                }
                state_ = ParseState::WaitingMagic;
                return frame;
            }
        }
    }
}

size_t StreamingFrameCodec::GetBufferSize() const {
    return buffer_.size();
}

void StreamingFrameCodec::Reset() {
    state_ = ParseState::WaitingMagic;
    buffer_.clear();
    partial_frame_ = Frame{};
    payload_length_ = 0;
}

bool StreamingFrameCodec::SyncMagic() {
    if (buffer_.size() < 4) {
        return false;
    }

    // 查找有效的 Magic
    for (size_t i = 0; i <= buffer_.size() - 4; ++i) {
        uint32_t magic = (static_cast<uint32_t>(buffer_[i]) << 24) |
                         (static_cast<uint32_t>(buffer_[i + 1]) << 16) |
                         (static_cast<uint32_t>(buffer_[i + 2]) << 8) |
                         static_cast<uint32_t>(buffer_[i + 3]);
        if (magic == kFrameMagic) {
            // 丢弃 Magic 前面的无效字节
            if (i > 0) {
                buffer_.erase(buffer_.begin(), buffer_.begin() + i);
            }
            return true;
        }
    }

    // 没有找到 Magic，保留最后 3 个字节（可能是不完整的 Magic 开头）
    if (buffer_.size() > 3) {
        buffer_.erase(buffer_.begin(), buffer_.begin() + (buffer_.size() - 3));
    }
    return false;
}

bool StreamingFrameCodec::ParseHeader() {
    if (buffer_.size() < kFrameHeaderSize) {
        return false;
    }

    // Version
    uint8_t version = buffer_[4];
    if (version != kFrameVersion) {
        // 版本不匹配，丢弃这个 Magic 重新同步
        buffer_.erase(buffer_.begin(), buffer_.begin() + 1);
        state_ = ParseState::WaitingMagic;
        return false;
    }

    // Message Type
    auto msg_type = static_cast<FrameMessageType>(buffer_[5]);

    // Length (big-endian)
    uint32_t length = (static_cast<uint32_t>(buffer_[6]) << 24) |
                      (static_cast<uint32_t>(buffer_[7]) << 16) |
                      (static_cast<uint32_t>(buffer_[8]) << 8) |
                      static_cast<uint32_t>(buffer_[9]);

    if (length > kMaxPayloadSize) {
        // Payload 长度非法，丢弃这个 Magic 重新同步
        buffer_.erase(buffer_.begin(), buffer_.begin() + 1);
        state_ = ParseState::WaitingMagic;
        return false;
    }

    partial_frame_.magic = kFrameMagic;
    partial_frame_.version = version;
    partial_frame_.msg_type = msg_type;
    partial_frame_.length = length;
    payload_length_ = length;
    return true;
}

bool StreamingFrameCodec::ParsePayload(Frame& out_frame) {
    size_t total_needed = kFrameHeaderSize + payload_length_;
    if (buffer_.size() < total_needed) {
        return false;
    }

    out_frame = partial_frame_;
    out_frame.payload.assign(
        buffer_.begin() + kFrameHeaderSize,
        buffer_.begin() + kFrameHeaderSize + payload_length_);

    // 从缓冲中移除已解析的帧
    buffer_.erase(buffer_.begin(), buffer_.begin() + total_needed);
    partial_frame_ = Frame{};
    payload_length_ = 0;
    return true;
}

} // namespace pmqueue
