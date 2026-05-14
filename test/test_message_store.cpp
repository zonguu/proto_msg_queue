#include <gtest/gtest.h>

#include <thread>
#include <chrono>

#include "storage/memory_message_store.h"

using namespace pmqueue;

TEST(MemoryMessageStoreTest, CreateDeleteTopic) {
    MemoryMessageStore store(1024 * 1024);

    EXPECT_TRUE(store.CreateTopic("topic1"));
    EXPECT_TRUE(store.HasTopic("topic1"));
    EXPECT_FALSE(store.CreateTopic("topic1")); // 重复创建

    EXPECT_TRUE(store.DeleteTopic("topic1"));
    EXPECT_FALSE(store.HasTopic("topic1"));
    EXPECT_FALSE(store.DeleteTopic("topic1")); // 删除不存在的
}

TEST(MemoryMessageStoreTest, PublishAndPull) {
    MemoryMessageStore store(1024 * 1024);

    store.CreateTopic("test_topic");

    Payload payload1 = {0x01, 0x02, 0x03};
    MessageId msg_id1 = 0;
    EXPECT_TRUE(store.Publish("test_topic", payload1, msg_id1));
    EXPECT_EQ(msg_id1, 1);

    Payload payload2 = {0x04, 0x05, 0x06};
    MessageId msg_id2 = 0;
    EXPECT_TRUE(store.Publish("test_topic", payload2, msg_id2));
    EXPECT_EQ(msg_id2, 2);

    // Pull 消息（独立消费者，is_group = false）
    auto messages = store.Pull("test_topic", "sub1", 10, false);
    ASSERT_EQ(messages.size(), 2);
    EXPECT_EQ(messages[0].id, 1);
    EXPECT_EQ(messages[0].payload, payload1);
    EXPECT_EQ(messages[1].id, 2);
    EXPECT_EQ(messages[1].payload, payload2);
}

TEST(MemoryMessageStoreTest, PullEmptyTopic) {
    MemoryMessageStore store(1024 * 1024);
    store.CreateTopic("empty_topic");

    auto messages = store.Pull("empty_topic", "sub1", 10, false);
    EXPECT_TRUE(messages.empty());
}

TEST(MemoryMessageStoreTest, PullNonExistentTopic) {
    MemoryMessageStore store(1024 * 1024);
    auto messages = store.Pull("no_such_topic", "sub1", 10, false);
    EXPECT_TRUE(messages.empty());
}

TEST(MemoryMessageStoreTest, AutoCreateTopicOnPublish) {
    MemoryMessageStore store(1024 * 1024);
    EXPECT_FALSE(store.HasTopic("auto_topic"));

    Payload payload = {0xAA, 0xBB};
    MessageId msg_id = 0;
    EXPECT_TRUE(store.Publish("auto_topic", payload, msg_id));
    EXPECT_TRUE(store.HasTopic("auto_topic"));
}

TEST(MemoryMessageStoreTest, LargePayload) {
    MemoryMessageStore store(1024 * 1024 * 10); // 10MB

    Payload large_payload(1024 * 1024, 0xCD); // 1MB
    MessageId msg_id = 0;
    EXPECT_TRUE(store.Publish("large_topic", large_payload, msg_id));

    auto messages = store.Pull("large_topic", "sub1", 1, false);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].payload.size(), 1024 * 1024);
}

TEST(MemoryMessageStoreTest, AckMessage) {
    MemoryMessageStore store(1024 * 1024);
    store.CreateTopic("ack_topic");

    Payload payload = {0x01};
    MessageId msg_id = 0;
    store.Publish("ack_topic", payload, msg_id);

    // 先拉取，再 ACK
    auto messages = store.Pull("ack_topic", "sub1", 10, false);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].id, msg_id);

    EXPECT_TRUE(store.Ack("ack_topic", "sub1", msg_id, false));
    
    // ACK 后再次拉取应该为空（offset 已推进）
    auto messages2 = store.Pull("ack_topic", "sub1", 10, false);
    EXPECT_TRUE(messages2.empty());
}

TEST(MemoryMessageStoreTest, BufferOverflow) {
    MemoryMessageStore store(128); // 很小的缓冲区
    store.CreateTopic("small_topic");

    Payload large_payload(200, 0xFF);
    MessageId msg_id = 0;
    EXPECT_FALSE(store.Publish("small_topic", large_payload, msg_id));
}

TEST(MemoryMessageStoreTest, ConsumerGroupOffsetIsolation) {
    MemoryMessageStore store(1024 * 1024);
    store.CreateTopic("group_topic");

    Payload payload = {0x01, 0x02};
    MessageId msg_id = 0;
    store.Publish("group_topic", payload, msg_id);

    // 消费者组 A 拉取
    auto msgs_a = store.Pull("group_topic", "group_a", 10, true);
    ASSERT_EQ(msgs_a.size(), 1);

    // 消费者组 B 也应该能拉取（组间隔离）
    auto msgs_b = store.Pull("group_topic", "group_b", 10, true);
    ASSERT_EQ(msgs_b.size(), 1);

    // 独立消费者也能拉取
    auto msgs_sub = store.Pull("group_topic", "sub1", 10, false);
    ASSERT_EQ(msgs_sub.size(), 1);
}

TEST(MemoryMessageStoreTest, RetryAndDlq) {
    // 使用非常短的重试参数以加速测试
    // pending_timeout_ms = 100ms, retry_interval_ms = 50ms, max_retry = 2
    MemoryMessageStore store(1024 * 1024, 2, 100, 50);
    store.CreateTopic("retry_topic");

    Payload payload = {0xAB};
    MessageId msg_id = 0;
    store.Publish("retry_topic", payload, msg_id);
    EXPECT_EQ(msg_id, 1);

    // 第 1 轮：拉取但不 ACK（retry_count = 0）
    auto messages = store.Pull("retry_topic", "sub1", 10, false);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].retry_count, 0);

    // 等待 pending 超时后重试
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // 第 2 轮：再次拉取（retry_count = 1），仍不 ACK
    auto messages2 = store.Pull("retry_topic", "sub1", 10, false);
    ASSERT_EQ(messages2.size(), 1);
    EXPECT_EQ(messages2[0].id, 1);
    EXPECT_EQ(messages2[0].retry_count, 1);

    // 等待再次超时后重试
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // 第 3 轮：再次拉取（retry_count = 2），仍不 ACK
    auto messages3 = store.Pull("retry_topic", "sub1", 10, false);
    ASSERT_EQ(messages3.size(), 1);
    EXPECT_EQ(messages3[0].id, 1);
    EXPECT_EQ(messages3[0].retry_count, 2);

    // 等待进入 DLQ（retry_count = 2 >= max_retry = 2）
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // 消息应该已进入 DLQ，不再能从主 Topic 拉取
    auto messages4 = store.Pull("retry_topic", "sub1", 10, false);
    EXPECT_TRUE(messages4.empty());

    // 从 DLQ 拉取
    auto dlq_messages = store.PullDlq("retry_topic", 10);
    ASSERT_EQ(dlq_messages.size(), 1);
    EXPECT_EQ(dlq_messages[0].id, 1);
}
