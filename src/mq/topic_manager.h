#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include <optional>
#include "common/non_copyable.h"
#include "mq/topic.h"

namespace pmqueue {

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

    // 订阅
    bool Subscribe(const std::string& topic_name, const SubscriberInfo& subscriber);

    // 取消订阅
    bool Unsubscribe(const std::string& topic_name, const SubscriberId& subscriber_id);

    // 获取订阅者列表
    std::vector<SubscriberInfo> GetSubscribers(const std::string& topic_name) const;

    // 检查是否存在
    bool HasTopic(const std::string& topic_name) const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TopicInfo> topics_;
    std::unordered_map<std::string, std::vector<SubscriberInfo>> subscribers_;
};

} // namespace pmqueue
