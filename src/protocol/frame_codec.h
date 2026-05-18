#pragma once

#include <cstdint>
#include <vector>
#include <optional>
#include <string>

#include "protocol/frame_protocol.h"

namespace pmqueue {

/**
 * @brief 帧编码/解码器
 * 
 * 编码：将 Frame 序列化为字节流
 * 解码：从字节流中解析完整的 Frame（解决 TCP 粘包问题）
 */
class FrameCodec {
public:
    FrameCodec() = default;

    // 编码 Frame 为字节流
    static std::vector<uint8_t> Encode(const Frame& frame);

    // 将帧头编码到预分配的缓冲区（用于零拷贝发送）
    // 返回实际写入的字节数（固定为 kFrameHeaderSize）
    static size_t EncodeHeaderToBuffer(const Frame& frame, uint8_t* buffer, size_t buffer_size);

    // 尝试从缓冲区解码一个 Frame
    // 返回解码后的 Frame，如果数据不足则返回 nullopt
    static std::optional<Frame> TryDecode(const std::vector<uint8_t>& buffer, size_t& consumed_bytes);

    // 检查缓冲区中是否有完整的帧
    static bool HasCompleteFrame(const std::vector<uint8_t>& buffer);

    // 获取帧头中的 payload 长度
    static std::optional<uint32_t> PeekPayloadLength(const std::vector<uint8_t>& buffer);

private:
    // 大小端转换
    static uint32_t HostToNetwork32(uint32_t value);
    static uint32_t NetworkToHost32(uint32_t value);
};

} // namespace pmqueue
