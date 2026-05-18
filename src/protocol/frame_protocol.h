#pragma once

#include <cstdint>
#include <vector>
#include <string>

namespace pmqueue {

// 帧协议常量
constexpr uint32_t kFrameMagic = 0xDEADBEEF;
constexpr uint8_t kFrameVersion = 0x01;
constexpr size_t kFrameHeaderSize = 10; // Magic(4) + Version(1) + MsgType(1) + Length(4)

// 消息类型
enum class FrameMessageType : uint8_t {
    Unknown = 0,
    Publish = 1,
    Subscribe = 2,
    Unsubscribe = 3,
    Pull = 4,
    Ack = 5,
    Response = 6,
    Push = 7,
    BatchPublish = 9,
    BatchPush = 10,
    Ping = 11,
    Pong = 12,
    Admin = 13,
};

// 帧结构
struct Frame {
    uint32_t magic = kFrameMagic;
    uint8_t version = kFrameVersion;
    FrameMessageType msg_type = FrameMessageType::Unknown;
    uint32_t length = 0;
    std::vector<uint8_t> payload;
};

} // namespace pmqueue
