#include <gtest/gtest.h>

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

    // Pull 消息
    auto messages = store.Pull("test_topic", "sub1", 10);
    ASSERT_EQ(messages.size(), 2);
    EXPECT_EQ(messages[0].id, 1);
    EXPECT_EQ(messages[0].payload, payload1);
    EXPECT_EQ(messages[1].id, 2);
    EXPECT_EQ(messages[1].payload, payload2);
}

TEST(MemoryMessageStoreTest, PullEmptyTopic) {
    MemoryMessageStore store(1024 * 1024);
    store.CreateTopic("empty_topic");

    auto messages = store.Pull("empty_topic", "sub1", 10);
    EXPECT_TRUE(messages.empty());
}

TEST(MemoryMessageStoreTest, PullNonExistentTopic) {
    MemoryMessageStore store(1024 * 1024);
    auto messages = store.Pull("no_such_topic", "sub1", 10);
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

    auto messages = store.Pull("large_topic", "sub1", 1);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].payload.size(), 1024 * 1024);
}

TEST(MemoryMessageStoreTest, AckMessage) {
    MemoryMessageStore store(1024 * 1024);
    store.CreateTopic("ack_topic");

    Payload payload = {0x01};
    MessageId msg_id = 0;
    store.Publish("ack_topic", payload, msg_id);

    EXPECT_TRUE(store.Ack("ack_topic", "sub1", msg_id));
}

TEST(MemoryMessageStoreTest, BufferOverflow) {
    MemoryMessageStore store(128); // 很小的缓冲区
    store.CreateTopic("small_topic");

    Payload large_payload(200, 0xFF);
    MessageId msg_id = 0;
    EXPECT_FALSE(store.Publish("small_topic", large_payload, msg_id));
}
