#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "protocol/frame_protocol.h"

namespace pmqueue {

/**
 * @brief 流式帧解码器
 * 
 * 基于状态机的帧解码器，支持：
 * - 逐步接收数据并解析（不需要完整帧在 buffer 中）
 * - Magic 自动同步（当数据错乱时自动跳过无效字节）
 * - 内部缓冲管理
 */
class StreamingFrameCodec {
public:
    StreamingFrameCodec() = default;

    /**
     * @brief 追加新读取的数据到内部缓冲
     */
    void AppendData(const uint8_t* data, size_t len);

    /**
     * @brief 尝试从内部缓冲解析一个完整帧
     * @return 如果有完整帧则返回并从缓冲中移除，否则返回 nullopt
     */
    std::optional<Frame> TryParseFrame();

    /**
     * @brief 获取当前内部缓冲大小
     */
    size_t GetBufferSize() const;

    /**
     * @brief 清空内部缓冲和状态
     */
    void Reset();

private:
    enum class ParseState {
        WaitingMagic,   // 等待 Magic 同步
        WaitingHeader,  // 等待完整帧头（10字节）
        WaitingPayload, // 等待完整 payload
    };

    // 尝试从 buffer 开头找到有效的 Magic 位置
    // 返回 true 表示找到了，false 表示数据不足
    bool SyncMagic();

    // 尝试解析帧头
    // 返回 true 表示成功，false 表示数据不足
    bool ParseHeader();

    // 尝试解析 payload
    // 返回 true 表示成功，false 表示数据不足
    bool ParsePayload(Frame& out_frame);

    ParseState state_ = ParseState::WaitingMagic;
    std::vector<uint8_t> buffer_;
    Frame partial_frame_;
    size_t payload_length_ = 0;
};

} // namespace pmqueue
