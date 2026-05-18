#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace pmqueue {

/**
 * @brief Topic 级别配置
 */
struct TopicConfig {
    size_t ring_buffer_size = 0;      // 0 = 使用全局默认值
    uint32_t default_ttl_ms = 0;      // 0 = 使用全局默认值
    bool compression_enabled = false; // 该 Topic 是否开启压缩
};

/**
 * @brief Broker 全局配置
 * 
 * 所有特性均可通过配置开启/关闭。
 */
struct BrokerConfig {
    // === 网络 ===
    uint16_t port = 9090;

    // === 批量读写 ===
    bool batch_publish_enabled = true;
    bool batch_push_enabled = true;
    uint32_t max_batch_size = 100;

    // === 心跳与连接保活 ===
    bool heartbeat_enabled = true;
    uint32_t heartbeat_interval_ms = 5000;
    uint32_t heartbeat_timeout_ms = 15000;
    uint32_t heartbeat_check_interval_ms = 5000;

    // === 背压与限流 ===
    bool rate_limit_enabled = true;
    uint32_t global_publish_rate = 10000;
    uint32_t conn_publish_rate = 1000;
    uint32_t rate_limit_burst = 100;
    size_t max_write_buffer_size = 8 * 1024 * 1024;

    // === 消息过期与 TTL ===
    bool ttl_enabled = true;
    uint32_t default_ttl_ms = 0;
    uint32_t expiration_check_interval_ms = 10000;

    // === 消息压缩 ===
    bool compression_enabled = false;
    uint32_t compression_threshold_bytes = 1024;

    // === 零拷贝 ===
    bool zero_copy_enabled = true;

    // === 消息去重 ===
    bool dedup_enabled = true;
    size_t dedup_window_size = 100000;

    // === 存储 ===
    size_t default_ring_buffer_size = 1024 * 1024;
    uint32_t max_retry_count = 3;
    uint32_t pending_timeout_ms = 5000;
    uint32_t retry_interval_ms = 1000;

    /**
     * @brief 从 JSON 文件加载配置
     * @param path 配置文件路径
     * @return true 成功，false 失败（失败时保持默认值）
     */
    bool LoadFromFile(const std::string& path);

    /**
     * @brief 保存当前配置到 JSON 文件
     */
    bool SaveToFile(const std::string& path) const;
};

/**
 * @brief 配置管理器（支持 Topic 级配置覆盖）
 */
class ConfigManager {
public:
    explicit ConfigManager(BrokerConfig global_config = BrokerConfig{});

    const BrokerConfig& GetGlobalConfig() const { return global_config_; }
    BrokerConfig& GetGlobalConfig() { return global_config_; }

    // Topic 级配置
    void SetTopicConfig(const std::string& topic, const TopicConfig& config);
    TopicConfig GetTopicConfig(const std::string& topic) const;
    bool HasTopicConfig(const std::string& topic) const;

private:
    BrokerConfig global_config_;
    std::unordered_map<std::string, TopicConfig> topic_configs_;
};

} // namespace pmqueue
