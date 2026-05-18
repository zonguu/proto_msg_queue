#include <gtest/gtest.h>
#include <fstream>
#include <cstdio>

#include "common/config.h"

using namespace pmqueue;

TEST(ConfigTest, DefaultValues) {
    BrokerConfig config;
    EXPECT_EQ(config.port, 9090);
    EXPECT_TRUE(config.batch_publish_enabled);
    EXPECT_TRUE(config.batch_push_enabled);
    EXPECT_TRUE(config.heartbeat_enabled);
    EXPECT_TRUE(config.rate_limit_enabled);
    EXPECT_TRUE(config.ttl_enabled);
    EXPECT_FALSE(config.compression_enabled);
    EXPECT_EQ(config.max_batch_size, 100);
}

TEST(ConfigTest, LoadFromFile) {
    // 创建临时配置文件
    const char* temp_file = "/tmp/pmq_test_config.json";
    {
        std::ofstream ofs(temp_file);
        ofs << "{\n"
            << "  \"port\": 9999,\n"
            << "  \"batch_publish_enabled\": false,\n"
            << "  \"heartbeat_enabled\": false,\n"
            << "  \"rate_limit_enabled\": false,\n"
            << "  \"ttl_enabled\": false,\n"
            << "  \"compression_enabled\": true,\n"
            << "  \"compression_threshold_bytes\": 2048,\n"
            << "  \"default_ttl_ms\": 60000\n"
            << "}\n";
    }

    BrokerConfig config;
    EXPECT_TRUE(config.LoadFromFile(temp_file));
    EXPECT_EQ(config.port, 9999);
    EXPECT_FALSE(config.batch_publish_enabled);
    EXPECT_FALSE(config.heartbeat_enabled);
    EXPECT_FALSE(config.rate_limit_enabled);
    EXPECT_FALSE(config.ttl_enabled);
    EXPECT_TRUE(config.compression_enabled);
    EXPECT_EQ(config.compression_threshold_bytes, 2048);
    EXPECT_EQ(config.default_ttl_ms, 60000);

    std::remove(temp_file);
}

TEST(ConfigTest, SaveAndLoadRoundTrip) {
    const char* temp_file = "/tmp/pmq_test_config2.json";
    
    BrokerConfig original;
    original.port = 7777;
    original.compression_enabled = true;
    original.max_batch_size = 50;
    original.global_publish_rate = 5000;
    
    EXPECT_TRUE(original.SaveToFile(temp_file));
    
    BrokerConfig loaded;
    EXPECT_TRUE(loaded.LoadFromFile(temp_file));
    EXPECT_EQ(loaded.port, 7777);
    EXPECT_TRUE(loaded.compression_enabled);
    EXPECT_EQ(loaded.max_batch_size, 50);
    EXPECT_EQ(loaded.global_publish_rate, 5000);
    
    std::remove(temp_file);
}

TEST(ConfigTest, ConfigManagerTopicConfig) {
    BrokerConfig global;
    global.compression_enabled = false;
    
    ConfigManager mgr(global);
    EXPECT_FALSE(mgr.GetGlobalConfig().compression_enabled);
    
    TopicConfig topic_cfg;
    topic_cfg.compression_enabled = true;
    topic_cfg.ring_buffer_size = 2 * 1024 * 1024;
    topic_cfg.default_ttl_ms = 30000;
    
    mgr.SetTopicConfig("my_topic", topic_cfg);
    EXPECT_TRUE(mgr.HasTopicConfig("my_topic"));
    
    auto retrieved = mgr.GetTopicConfig("my_topic");
    EXPECT_TRUE(retrieved.compression_enabled);
    EXPECT_EQ(retrieved.ring_buffer_size, 2 * 1024 * 1024);
    EXPECT_EQ(retrieved.default_ttl_ms, 30000);
    
    // 未配置的 Topic 返回默认值
    auto default_cfg = mgr.GetTopicConfig("other_topic");
    EXPECT_FALSE(default_cfg.compression_enabled);
    EXPECT_EQ(default_cfg.ring_buffer_size, 0);
}
