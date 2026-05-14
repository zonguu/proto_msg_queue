#include <gtest/gtest.h>

#include "mq/topic_manager.h"

using namespace pmqueue;

TEST(TopicManagerTest, RegisterTopic) {
    TopicManager tm;

    TopicInfo topic;
    topic.name = "topic1";
    topic.create_time = 12345;
    topic.message_count = 0;

    EXPECT_TRUE(tm.RegisterTopic(topic));
    EXPECT_TRUE(tm.HasTopic("topic1"));
    EXPECT_FALSE(tm.RegisterTopic(topic)); // 重复注册
}

TEST(TopicManagerTest, UnregisterTopic) {
    TopicManager tm;

    TopicInfo topic;
    topic.name = "topic1";
    tm.RegisterTopic(topic);

    EXPECT_TRUE(tm.UnregisterTopic("topic1"));
    EXPECT_FALSE(tm.HasTopic("topic1"));
    EXPECT_TRUE(tm.UnregisterTopic("topic1")); // 删除不存在的
}

TEST(TopicManagerTest, GetTopic) {
    TopicManager tm;

    TopicInfo topic;
    topic.name = "topic1";
    topic.create_time = 12345;
    tm.RegisterTopic(topic);

    auto result = tm.GetTopic("topic1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->name, "topic1");
    EXPECT_EQ(result->create_time, 12345);

    auto no_result = tm.GetTopic("no_such");
    EXPECT_FALSE(no_result.has_value());
}

TEST(TopicManagerTest, GetAllTopics) {
    TopicManager tm;

    for (int i = 0; i < 5; ++i) {
        TopicInfo topic;
        topic.name = "topic" + std::to_string(i);
        tm.RegisterTopic(topic);
    }

    auto topics = tm.GetAllTopics();
    EXPECT_EQ(topics.size(), 5);
}

TEST(TopicManagerTest, SubscribeUnsubscribe) {
    TopicManager tm;

    TopicInfo topic;
    topic.name = "topic1";
    tm.RegisterTopic(topic);

    SubscriberInfo sub;
    sub.id = "sub1";
    sub.conn_id = 1;

    EXPECT_TRUE(tm.Subscribe("topic1", sub));
    EXPECT_FALSE(tm.Subscribe("topic1", sub)); // 重复订阅

    auto subs = tm.GetSubscribers("topic1");
    ASSERT_EQ(subs.size(), 1);
    EXPECT_EQ(subs[0].id, "sub1");

    EXPECT_TRUE(tm.Unsubscribe("topic1", "sub1"));
    subs = tm.GetSubscribers("topic1");
    EXPECT_TRUE(subs.empty());
    EXPECT_FALSE(tm.Unsubscribe("topic1", "sub1")); // 再次取消
}

TEST(TopicManagerTest, SubscribeAutoCreateTopic) {
    TopicManager tm;
    EXPECT_FALSE(tm.HasTopic("auto_topic"));

    SubscriberInfo sub;
    sub.id = "sub1";
    sub.conn_id = 1;

    EXPECT_TRUE(tm.Subscribe("auto_topic", sub));
    EXPECT_TRUE(tm.HasTopic("auto_topic"));
}

TEST(TopicManagerTest, MultipleSubscribers) {
    TopicManager tm;

    TopicInfo topic;
    topic.name = "topic1";
    tm.RegisterTopic(topic);

    for (int i = 0; i < 10; ++i) {
        SubscriberInfo sub;
        sub.id = "sub" + std::to_string(i);
        sub.conn_id = i;
        EXPECT_TRUE(tm.Subscribe("topic1", sub));
    }

    auto subs = tm.GetSubscribers("topic1");
    EXPECT_EQ(subs.size(), 10);
}

// ============================================================================
// 消费者组测试
// ============================================================================

TEST(TopicManagerTest, ConsumerGroupSubscribe) {
    TopicManager tm;

    SubscriberInfo sub1;
    sub1.id = "member1";
    sub1.conn_id = 1;

    SubscriberInfo sub2;
    sub2.id = "member2";
    sub2.conn_id = 2;

    // 加入同一消费者组
    EXPECT_TRUE(tm.Subscribe("topic1", sub1, "group_a"));
    EXPECT_TRUE(tm.Subscribe("topic1", sub2, "group_a"));
    EXPECT_FALSE(tm.Subscribe("topic1", sub1, "group_a")); // 重复加入

    auto groups = tm.GetConsumerGroups("topic1");
    ASSERT_EQ(groups.size(), 1);
    EXPECT_EQ(groups[0].group_id, "group_a");
    EXPECT_EQ(groups[0].members.size(), 2);
}

TEST(TopicManagerTest, ConsumerGroupUnsubscribe) {
    TopicManager tm;

    tm.Subscribe("topic1", SubscriberInfo{"m1", 1}, "group_a");
    tm.Subscribe("topic1", SubscriberInfo{"m2", 2}, "group_a");

    EXPECT_TRUE(tm.Unsubscribe("topic1", "m1", "group_a"));
    auto groups = tm.GetConsumerGroups("topic1");
    ASSERT_EQ(groups.size(), 1);
    EXPECT_EQ(groups[0].members.size(), 1);

    // 退完所有成员后组应被删除
    EXPECT_TRUE(tm.Unsubscribe("topic1", "m2", "group_a"));
    auto groups2 = tm.GetConsumerGroups("topic1");
    EXPECT_TRUE(groups2.empty());
}

TEST(TopicManagerTest, ConsumerGroupRoundRobin) {
    TopicManager tm;

    tm.Subscribe("topic1", SubscriberInfo{"m1", 1}, "group_a");
    tm.Subscribe("topic1", SubscriberInfo{"m2", 2}, "group_a");
    tm.Subscribe("topic1", SubscriberInfo{"m3", 3}, "group_a");

    // Round-Robin 选择
    EXPECT_EQ(tm.SelectNextMember("topic1", "group_a"), "m1");
    EXPECT_EQ(tm.SelectNextMember("topic1", "group_a"), "m2");
    EXPECT_EQ(tm.SelectNextMember("topic1", "group_a"), "m3");
    EXPECT_EQ(tm.SelectNextMember("topic1", "group_a"), "m1"); // 循环
}

TEST(TopicManagerTest, BroadcastAndGroupCoexist) {
    TopicManager tm;

    // 广播订阅者
    tm.Subscribe("topic1", SubscriberInfo{"broadcast_sub", 100});

    // 消费者组成员
    tm.Subscribe("topic1", SubscriberInfo{"g1_m1", 1}, "group1");
    tm.Subscribe("topic1", SubscriberInfo{"g1_m2", 2}, "group1");

    auto subs = tm.GetSubscribers("topic1");
    EXPECT_EQ(subs.size(), 1);
    EXPECT_EQ(subs[0].id, "broadcast_sub");

    auto groups = tm.GetConsumerGroups("topic1");
    EXPECT_EQ(groups.size(), 1);
    EXPECT_EQ(groups[0].members.size(), 2);
}
