#include "protocol/frame_codec.h"

#include <arpa/inet.h>
#include <cstring>

#include "common/types.h"

namespace pmqueue {

std::vector<uint8_t> FrameCodec::Encode(const Frame& frame) {
    std::vector<uint8_t> result;
    result.reserve(kFrameHeaderSize + frame.payload.size());

    // Magic (4 bytes, big-endian) - 手动编码，不依赖 htonl
    result.push_back(static_cast<uint8_t>((frame.magic >> 24) & 0xFF));
    result.push_back(static_cast<uint8_t>((frame.magic >> 16) & 0xFF));
    result.push_back(static_cast<uint8_t>((frame.magic >> 8) & 0xFF));
    result.push_back(static_cast<uint8_t>(frame.magic & 0xFF));

    // Version (1 byte)
    result.push_back(frame.version);

    // Message Type (1 byte)
    result.push_back(static_cast<uint8_t>(frame.msg_type));

    // Length (4 bytes, big-endian)
    uint32_t length = static_cast<uint32_t>(frame.payload.size());
    result.push_back(static_cast<uint8_t>((length >> 24) & 0xFF));
    result.push_back(static_cast<uint8_t>((length >> 16) & 0xFF));
    result.push_back(static_cast<uint8_t>((length >> 8) & 0xFF));
    result.push_back(static_cast<uint8_t>(length & 0xFF));

    // Payload
    result.insert(result.end(), frame.payload.begin(), frame.payload.end());

    return result;
}

std::optional<Frame> FrameCodec::TryDecode(const std::vector<uint8_t>& buffer, size_t& consumed_bytes) {
    consumed_bytes = 0;

    if (buffer.size() < kFrameHeaderSize) {
        return std::nullopt;
    }

    // 解析 Magic (big-endian)
    uint32_t magic = (static_cast<uint32_t>(buffer[0]) << 24) |
                     (static_cast<uint32_t>(buffer[1]) << 16) |
                     (static_cast<uint32_t>(buffer[2]) << 8) |
                     static_cast<uint32_t>(buffer[3]);

    if (magic != kFrameMagic) {
        // 魔术字不匹配，可能是数据错乱，尝试丢弃第一个字节重新查找
        // 为简化处理，这里直接返回错误，让上层处理
        return std::nullopt;
    }

    // Version
    uint8_t version = buffer[4];
    if (version != kFrameVersion) {
        return std::nullopt;
    }

    // Message Type
    auto msg_type = static_cast<FrameMessageType>(buffer[5]);

    // Length (big-endian)
    uint32_t length = (static_cast<uint32_t>(buffer[6]) << 24) |
                      (static_cast<uint32_t>(buffer[7]) << 16) |
                      (static_cast<uint32_t>(buffer[8]) << 8) |
                      static_cast<uint32_t>(buffer[9]);

    if (length > kMaxPayloadSize) {
        return std::nullopt;
    }

    // 检查是否有完整的 payload
    if (buffer.size() < kFrameHeaderSize + length) {
        return std::nullopt;
    }

    Frame frame;
    frame.magic = magic;
    frame.version = version;
    frame.msg_type = msg_type;
    frame.length = length;
    frame.payload.assign(buffer.begin() + kFrameHeaderSize, buffer.begin() + kFrameHeaderSize + length);

    consumed_bytes = kFrameHeaderSize + length;
    return frame;
}

bool FrameCodec::HasCompleteFrame(const std::vector<uint8_t>& buffer) {
    if (buffer.size() < kFrameHeaderSize) {
        return false;
    }

    uint32_t length = (static_cast<uint32_t>(buffer[6]) << 24) |
                      (static_cast<uint32_t>(buffer[7]) << 16) |
                      (static_cast<uint32_t>(buffer[8]) << 8) |
                      static_cast<uint32_t>(buffer[9]);

    return buffer.size() >= kFrameHeaderSize + length;
}

std::optional<uint32_t> FrameCodec::PeekPayloadLength(const std::vector<uint8_t>& buffer) {
    if (buffer.size() < kFrameHeaderSize) {
        return std::nullopt;
    }

    uint32_t length = (static_cast<uint32_t>(buffer[6]) << 24) |
                      (static_cast<uint32_t>(buffer[7]) << 16) |
                      (static_cast<uint32_t>(buffer[8]) << 8) |
                      static_cast<uint32_t>(buffer[9]);
    return length;
}

uint32_t FrameCodec::HostToNetwork32(uint32_t value) {
    return htonl(value);
}

uint32_t FrameCodec::NetworkToHost32(uint32_t value) {
    return ntohl(value);
}

} // namespace pmqueue
