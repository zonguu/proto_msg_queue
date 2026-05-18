#pragma once

#include <cstdint>
#include <string>

namespace pmqueue {

/**
 * @brief 标准化错误码枚举
 * 
 * 所有 Broker 返回的错误均通过此枚举标识，客户端可程序化判断错误类型。
 */
enum class ErrorCode : uint32_t {
    Success = 0,
    Unknown = 1,
    InvalidRequest = 2,
    TopicNotFound = 3,
    TopicAlreadyExists = 4,
    NotSubscribed = 5,
    AlreadySubscribed = 6,
    RateLimitExceeded = 7,
    BufferFull = 8,
    MessageNotFound = 9,
    DuplicateMessage = 10,
    BatchSizeExceeded = 11,
    AdminCommandFailed = 12,
};

/**
 * @brief 将错误码转换为可读字符串
 */
inline std::string ErrorCodeToString(ErrorCode code) {
    switch (code) {
        case ErrorCode::Success: return "Success";
        case ErrorCode::Unknown: return "Unknown error";
        case ErrorCode::InvalidRequest: return "Invalid request";
        case ErrorCode::TopicNotFound: return "Topic not found";
        case ErrorCode::TopicAlreadyExists: return "Topic already exists";
        case ErrorCode::NotSubscribed: return "Not subscribed";
        case ErrorCode::AlreadySubscribed: return "Already subscribed";
        case ErrorCode::RateLimitExceeded: return "Rate limit exceeded";
        case ErrorCode::BufferFull: return "Buffer full";
        case ErrorCode::MessageNotFound: return "Message not found";
        case ErrorCode::DuplicateMessage: return "Duplicate message";
        case ErrorCode::BatchSizeExceeded: return "Batch size exceeded";
        case ErrorCode::AdminCommandFailed: return "Admin command failed";
        default: return "Unknown error code";
    }
}

} // namespace pmqueue
