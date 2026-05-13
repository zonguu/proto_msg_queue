#include "mq/topic_manager.h"

#include <algorithm>

namespace pmqueue {

TopicManager::TopicManager() = default;

bool TopicManager::RegisterTopic(const TopicInfo& topic) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (topics_.find(topic.name) != topics_.end()) {
        return false;
    }
    topics_[topic.name] = topic;
    return true;
}

bool TopicManager::UnregisterTopic(const std::string& topic_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    topics_.erase(topic_name);
    subscribers_.erase(topic_name);
    return true;
}

std::optional<TopicInfo> TopicManager::GetTopic(const std::string& topic_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topics_.find(topic_name);
    if (it != topics_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::vector<TopicInfo> TopicManager::GetAllTopics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<TopicInfo> result;
    result.reserve(topics_.size());
    for (const auto& [name, topic] : topics_) {
        result.push_back(topic);
    }
    return result;
}

bool TopicManager::Subscribe(const std::string& topic_name, const SubscriberInfo& subscriber) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (topics_.find(topic_name) == topics_.end()) {
        // 自动创建 Topic
        TopicInfo topic;
        topic.name = topic_name;
        topic.create_time = 0;
        topic.message_count = 0;
        topics_[topic_name] = topic;
    }

    auto& subs = subscribers_[topic_name];
    // 检查是否已存在
    auto it = std::find_if(subs.begin(), subs.end(),
        [&subscriber](const SubscriberInfo& info) { return info.id == subscriber.id; });
    if (it != subs.end()) {
        return false;
    }

    subs.push_back(subscriber);
    return true;
}

bool TopicManager::Unsubscribe(const std::string& topic_name, const SubscriberId& subscriber_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = subscribers_.find(topic_name);
    if (it == subscribers_.end()) {
        return false;
    }

    auto& subs = it->second;
    auto sub_it = std::remove_if(subs.begin(), subs.end(),
        [&subscriber_id](const SubscriberInfo& info) { return info.id == subscriber_id; });
    if (sub_it == subs.end()) {
        return false;
    }

    subs.erase(sub_it, subs.end());
    return true;
}

std::vector<SubscriberInfo> TopicManager::GetSubscribers(const std::string& topic_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = subscribers_.find(topic_name);
    if (it != subscribers_.end()) {
        return it->second;
    }
    return {};
}

bool TopicManager::HasTopic(const std::string& topic_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return topics_.find(topic_name) != topics_.end();
}

} // namespace pmqueue
