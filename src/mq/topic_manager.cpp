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
    consumer_groups_.erase(topic_name);
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

bool TopicManager::Subscribe(
    const std::string& topic_name, 
    const SubscriberInfo& subscriber, 
    const std::string& group_id) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 自动创建 Topic
    if (topics_.find(topic_name) == topics_.end()) {
        TopicInfo topic;
        topic.name = topic_name;
        topic.create_time = 0;
        topic.message_count = 0;
        topics_[topic_name] = topic;
    }

    if (group_id.empty()) {
        // 广播订阅
        auto& subs = subscribers_[topic_name];
        auto it = std::find_if(subs.begin(), subs.end(),
            [&subscriber](const SubscriberInfo& info) { return info.id == subscriber.id; });
        if (it != subs.end()) {
            return false; // 已存在
        }
        subs.push_back(subscriber);
    } else {
        // 消费者组订阅
        auto& groups = consumer_groups_[topic_name];
        auto group_it = groups.find(group_id);
        if (group_it == groups.end()) {
            // 创建新消费者组
            ConsumerGroup group;
            group.group_id = group_id;
            group.topic_name = topic_name;
            group.members.push_back({subscriber.id, subscriber.conn_id});
            groups[group_id] = std::move(group);
        } else {
            // 加入已有组
            auto& members = group_it->second.members;
            auto member_it = std::find_if(members.begin(), members.end(),
                [&subscriber](const ConsumerGroupMember& m) { return m.subscriber_id == subscriber.id; });
            if (member_it != members.end()) {
                return false; // 已存在
            }
            members.push_back({subscriber.id, subscriber.conn_id});
        }
    }
    
    return true;
}

bool TopicManager::Unsubscribe(
    const std::string& topic_name, 
    const SubscriberId& subscriber_id, 
    const std::string& group_id) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (group_id.empty()) {
        // 广播订阅取消
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
    } else {
        // 消费者组取消
        auto topic_it = consumer_groups_.find(topic_name);
        if (topic_it == consumer_groups_.end()) {
            return false;
        }
        auto group_it = topic_it->second.find(group_id);
        if (group_it == topic_it->second.end()) {
            return false;
        }
        auto& members = group_it->second.members;
        auto member_it = std::remove_if(members.begin(), members.end(),
            [&subscriber_id](const ConsumerGroupMember& m) { return m.subscriber_id == subscriber_id; });
        if (member_it == members.end()) {
            return false;
        }
        members.erase(member_it, members.end());
        
        // 如果组内无成员，删除该组
        if (members.empty()) {
            topic_it->second.erase(group_it);
        }
    }
    
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

std::vector<ConsumerGroup> TopicManager::GetConsumerGroups(const std::string& topic_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = consumer_groups_.find(topic_name);
    if (it != consumer_groups_.end()) {
        std::vector<ConsumerGroup> result;
        for (const auto& [group_id, group] : it->second) {
            result.push_back(group);
        }
        return result;
    }
    return {};
}

std::optional<ConsumerGroup> TopicManager::GetConsumerGroup(
    const std::string& topic_name, 
    const std::string& group_id) const {
    
    std::lock_guard<std::mutex> lock(mutex_);
    auto topic_it = consumer_groups_.find(topic_name);
    if (topic_it == consumer_groups_.end()) {
        return std::nullopt;
    }
    auto group_it = topic_it->second.find(group_id);
    if (group_it == topic_it->second.end()) {
        return std::nullopt;
    }
    return group_it->second;
}

std::string TopicManager::SelectNextMember(const std::string& topic_name, const std::string& group_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto topic_it = consumer_groups_.find(topic_name);
    if (topic_it == consumer_groups_.end()) {
        return "";
    }
    auto group_it = topic_it->second.find(group_id);
    if (group_it == topic_it->second.end() || group_it->second.members.empty()) {
        return "";
    }
    
    auto& group = group_it->second;
    const size_t idx = group.round_robin_index % group.members.size();
    group.round_robin_index = (group.round_robin_index + 1) % group.members.size();
    return group.members[idx].subscriber_id;
}

bool TopicManager::HasTopic(const std::string& topic_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return topics_.find(topic_name) != topics_.end();
}

bool TopicManager::UnsubscribeByConnId(ConnectionId conn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    bool removed_any = false;

    // 清理广播订阅者
    for (auto it = subscribers_.begin(); it != subscribers_.end(); ) {
        auto& subs = it->second;
        auto before_size = subs.size();
        subs.erase(
            std::remove_if(subs.begin(), subs.end(),
                [conn_id](const SubscriberInfo& info) { return info.conn_id == conn_id; }),
            subs.end());
        if (subs.size() < before_size) {
            removed_any = true;
        }
        if (subs.empty()) {
            it = subscribers_.erase(it);
        } else {
            ++it;
        }
    }

    // 清理消费者组成员
    for (auto topic_it = consumer_groups_.begin(); topic_it != consumer_groups_.end(); ) {
        auto& groups = topic_it->second;
        for (auto group_it = groups.begin(); group_it != groups.end(); ) {
            auto& members = group_it->second.members;
            auto before_size = members.size();
            members.erase(
                std::remove_if(members.begin(), members.end(),
                    [conn_id](const ConsumerGroupMember& m) { return m.conn_id == conn_id; }),
                members.end());
            if (members.size() < before_size) {
                removed_any = true;
            }
            if (members.empty()) {
                group_it = groups.erase(group_it);
            } else {
                ++group_it;
            }
        }
        if (groups.empty()) {
            topic_it = consumer_groups_.erase(topic_it);
        } else {
            ++topic_it;
        }
    }

    return removed_any;
}

} // namespace pmqueue
