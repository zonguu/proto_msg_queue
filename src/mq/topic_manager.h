#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "common/non_copyable.h"
#include "mq/topic.h"

namespace pmqueue {

/**
 * @brief 消费者组成员信息
 */
struct ConsumerGroupMember {
    SubscriberId subscriber_id;
    ConnectionId conn_id;
};

/**
 * @brief 消费者组
 */
struct ConsumerGroup {
    std::string group_id;
    std::string topic_name;
    std::vector<ConsumerGroupMember> members;
    // 轮询索引：用于 Round-Robin 分配消息给组内成员
    size_t round_robin_index = 0;
};

/**
 * @brief Topic 管理器（支持广播订阅与消费者组）
 * 
 * 订阅模式：
 * - 广播模式：subscriber 不指定 group_id，消息推送给所有订阅者
 * - 消费者组模式：subscriber 指定 group_id，消息在组内轮询分配
 */
class TopicManager : public NonCopyable {
public:
    TopicManager();
    ~TopicManager() = default;

    // 注册 Topic
    bool RegisterTopic(const TopicInfo& topic);

    // 注销 Topic
    bool UnregisterTopic(const std::string& topic_name);

    // 获取 Topic
    std::optional<TopicInfo> GetTopic(const std::string& topic_name) const;

    // 获取所有 Topic
    std::vector<TopicInfo> GetAllTopics() const;

    /**
     * @brief 订阅 Topic
     * @param topic_name Topic 名称
     * @param subscriber 订阅者信息
     * @param group_id 消费者组 ID，为空表示广播订阅
     */
    bool Subscribe(const std::string& topic_name, const SubscriberInfo& subscriber, const std::string& group_id = "");

    /**
     * @brief 取消订阅
     * @param topic_name Topic 名称
     * @param subscriber_id 订阅者 ID
     * @param group_id 消费者组 ID，为空表示广播订阅
     */
    bool Unsubscribe(const std::string& topic_name, const SubscriberId& subscriber_id, const std::string& group_id = "");

    // 获取广播订阅者列表
    std::vector<SubscriberInfo> GetSubscribers(const std::string& topic_name) const;

    /**
     * @brief 获取消费者组列表
     */
    std::vector<ConsumerGroup> GetConsumerGroups(const std::string& topic_name) const;

    /**
     * @brief 获取指定消费者组
     */
    std::optional<ConsumerGroup> GetConsumerGroup(const std::string& topic_name, const std::string& group_id) const;

    /**
     * @brief 为消费者组选择下一个接收消息的 member（Round-Robin）
     * @return 选中的成员 subscriber_id，如果组为空返回空字符串
     */
    std::string SelectNextMember(const std::string& topic_name, const std::string& group_id);

    // 检查是否存在
    bool HasTopic(const std::string& topic_name) const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TopicInfo> topics_;
    // 广播订阅者：topic -> [subscribers]
    std::unordered_map<std::string, std::vector<SubscriberInfo>> subscribers_;
    // 消费者组：topic -> {group_id -> ConsumerGroup}
    std::unordered_map<std::string, std::unordered_map<std::string, ConsumerGroup>> consumer_groups_;
};

} // namespace pmqueue
