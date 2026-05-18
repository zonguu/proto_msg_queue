#include "common/config.h"

#include <fstream>
#include <sstream>
#include <cstdlib>

namespace pmqueue {

// ============================================================================
// 简易 JSON 解析辅助函数
// ============================================================================

static std::string Trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

static bool ParseBool(const std::string& value, bool& out) {
    std::string v = Trim(value);
    if (v == "true" || v == "1") { out = true; return true; }
    if (v == "false" || v == "0") { out = false; return true; }
    return false;
}

static bool ParseUint32(const std::string& value, uint32_t& out) {
    try {
        out = static_cast<uint32_t>(std::stoul(Trim(value)));
        return true;
    } catch (...) {
        return false;
    }
}

static bool ParseSizeT(const std::string& value, size_t& out) {
    try {
        out = static_cast<size_t>(std::stoull(Trim(value)));
        return true;
    } catch (...) {
        return false;
    }
}

static bool ParseUint16(const std::string& value, uint16_t& out) {
    try {
        out = static_cast<uint16_t>(std::stoul(Trim(value)));
        return true;
    } catch (...) {
        return false;
    }
}

static std::string ExtractValue(const std::string& line, const std::string& key) {
    size_t pos = line.find('"' + key + '"');
    if (pos == std::string::npos) return "";
    
    pos = line.find(':', pos);
    if (pos == std::string::npos) return "";
    
    size_t comma = line.find(',', pos);
    if (comma == std::string::npos) {
        comma = line.find('}', pos);
    }
    if (comma == std::string::npos) {
        comma = line.length();
    }
    
    return Trim(line.substr(pos + 1, comma - pos - 1));
}

// ============================================================================
// BrokerConfig
// ============================================================================

bool BrokerConfig::LoadFromFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return false;
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();

    // 移除注释和空白（非常简化的处理）
    std::string json;
    bool in_string = false;
    for (size_t i = 0; i < content.size(); ++i) {
        char c = content[i];
        if (c == '"' && (i == 0 || content[i-1] != '\\')) {
            in_string = !in_string;
        }
        if (!in_string && c == '/' && i + 1 < content.size() && content[i+1] == '/') {
            // 跳过行注释
            while (i < content.size() && content[i] != '\n') ++i;
            continue;
        }
        json += c;
    }

    auto GetVal = [&](const std::string& key, auto& field, auto parser) -> bool {
        std::string val = ExtractValue(json, key);
        if (val.empty()) return false;
        return parser(val, field);
    };

    // 网络
    uint16_t port_tmp = 0;
    if (GetVal("port", port_tmp, ParseUint16)) port = port_tmp;

    // 批量读写
    ParseBool(ExtractValue(json, "batch_publish_enabled"), batch_publish_enabled);
    ParseBool(ExtractValue(json, "batch_push_enabled"), batch_push_enabled);
    uint32_t max_batch = 0;
    if (GetVal("max_batch_size", max_batch, ParseUint32)) max_batch_size = max_batch;

    // 心跳
    ParseBool(ExtractValue(json, "heartbeat_enabled"), heartbeat_enabled);
    uint32_t hb_interval = 0, hb_timeout = 0, hb_check = 0;
    if (GetVal("heartbeat_interval_ms", hb_interval, ParseUint32)) heartbeat_interval_ms = hb_interval;
    if (GetVal("heartbeat_timeout_ms", hb_timeout, ParseUint32)) heartbeat_timeout_ms = hb_timeout;
    if (GetVal("heartbeat_check_interval_ms", hb_check, ParseUint32)) heartbeat_check_interval_ms = hb_check;

    // 背压限流
    ParseBool(ExtractValue(json, "rate_limit_enabled"), rate_limit_enabled);
    uint32_t global_rate = 0, conn_rate = 0, burst = 0;
    if (GetVal("global_publish_rate", global_rate, ParseUint32)) global_publish_rate = global_rate;
    if (GetVal("conn_publish_rate", conn_rate, ParseUint32)) conn_publish_rate = conn_rate;
    if (GetVal("rate_limit_burst", burst, ParseUint32)) rate_limit_burst = burst;
    size_t max_write = 0;
    if (GetVal("max_write_buffer_size", max_write, ParseSizeT)) max_write_buffer_size = max_write;

    // TTL
    ParseBool(ExtractValue(json, "ttl_enabled"), ttl_enabled);
    uint32_t ttl = 0, exp_check = 0;
    if (GetVal("default_ttl_ms", ttl, ParseUint32)) default_ttl_ms = ttl;
    if (GetVal("expiration_check_interval_ms", exp_check, ParseUint32)) expiration_check_interval_ms = exp_check;

    // 压缩
    ParseBool(ExtractValue(json, "compression_enabled"), compression_enabled);
    uint32_t comp_thresh = 0;
    if (GetVal("compression_threshold_bytes", comp_thresh, ParseUint32)) compression_threshold_bytes = comp_thresh;

    // 存储
    size_t ring_size = 0;
    if (GetVal("default_ring_buffer_size", ring_size, ParseSizeT)) default_ring_buffer_size = ring_size;
    uint32_t max_retry = 0, pending_to = 0, retry_int = 0;
    if (GetVal("max_retry_count", max_retry, ParseUint32)) max_retry_count = max_retry;
    if (GetVal("pending_timeout_ms", pending_to, ParseUint32)) pending_timeout_ms = pending_to;
    if (GetVal("retry_interval_ms", retry_int, ParseUint32)) retry_interval_ms = retry_int;

    return true;
}

bool BrokerConfig::SaveToFile(const std::string& path) const {
    std::ofstream file(path);
    if (!file.is_open()) {
        return false;
    }

    file << "{\n";
    file << "  \"port\": " << port << ",\n";
    file << "  \"batch_publish_enabled\": " << (batch_publish_enabled ? "true" : "false") << ",\n";
    file << "  \"batch_push_enabled\": " << (batch_push_enabled ? "true" : "false") << ",\n";
    file << "  \"max_batch_size\": " << max_batch_size << ",\n";
    file << "  \"heartbeat_enabled\": " << (heartbeat_enabled ? "true" : "false") << ",\n";
    file << "  \"heartbeat_interval_ms\": " << heartbeat_interval_ms << ",\n";
    file << "  \"heartbeat_timeout_ms\": " << heartbeat_timeout_ms << ",\n";
    file << "  \"heartbeat_check_interval_ms\": " << heartbeat_check_interval_ms << ",\n";
    file << "  \"rate_limit_enabled\": " << (rate_limit_enabled ? "true" : "false") << ",\n";
    file << "  \"global_publish_rate\": " << global_publish_rate << ",\n";
    file << "  \"conn_publish_rate\": " << conn_publish_rate << ",\n";
    file << "  \"rate_limit_burst\": " << rate_limit_burst << ",\n";
    file << "  \"max_write_buffer_size\": " << max_write_buffer_size << ",\n";
    file << "  \"ttl_enabled\": " << (ttl_enabled ? "true" : "false") << ",\n";
    file << "  \"default_ttl_ms\": " << default_ttl_ms << ",\n";
    file << "  \"expiration_check_interval_ms\": " << expiration_check_interval_ms << ",\n";
    file << "  \"compression_enabled\": " << (compression_enabled ? "true" : "false") << ",\n";
    file << "  \"compression_threshold_bytes\": " << compression_threshold_bytes << ",\n";
    file << "  \"default_ring_buffer_size\": " << default_ring_buffer_size << ",\n";
    file << "  \"max_retry_count\": " << max_retry_count << ",\n";
    file << "  \"pending_timeout_ms\": " << pending_timeout_ms << ",\n";
    file << "  \"retry_interval_ms\": " << retry_interval_ms << "\n";
    file << "}\n";

    return true;
}

// ============================================================================
// ConfigManager
// ============================================================================

ConfigManager::ConfigManager(BrokerConfig global_config)
    : global_config_(std::move(global_config)) {}

void ConfigManager::SetTopicConfig(const std::string& topic, const TopicConfig& config) {
    topic_configs_[topic] = config;
}

TopicConfig ConfigManager::GetTopicConfig(const std::string& topic) const {
    auto it = topic_configs_.find(topic);
    if (it != topic_configs_.end()) {
        return it->second;
    }
    // 返回默认 TopicConfig
    return TopicConfig{};
}

bool ConfigManager::HasTopicConfig(const std::string& topic) const {
    return topic_configs_.find(topic) != topic_configs_.end();
}

} // namespace pmqueue
