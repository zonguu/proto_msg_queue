#include <gtest/gtest.h>

#include "protocol/streaming_frame_codec.h"
#include "protocol/frame_codec.h"
#include "protocol/frame_protocol.h"

using namespace pmqueue;

TEST(StreamingFrameCodecTest, BasicEncodeThenParse) {
    Frame frame;
    frame.msg_type = FrameMessageType::Publish;
    frame.payload = {0x01, 0x02, 0x03};

    auto encoded = FrameCodec::Encode(frame);

    StreamingFrameCodec codec;
    codec.AppendData(encoded.data(), encoded.size());

    auto result = codec.TryParseFrame();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->msg_type, FrameMessageType::Publish);
    EXPECT_EQ(result->payload, frame.payload);
}

TEST(StreamingFrameCodecTest, ChunkedAppend) {
    Frame frame;
    frame.msg_type = FrameMessageType::Response;
    frame.payload = {0xAA, 0xBB, 0xCC, 0xDD};

    auto encoded = FrameCodec::Encode(frame);

    StreamingFrameCodec codec;
    // 分两次追加
    codec.AppendData(encoded.data(), 5);
    auto r1 = codec.TryParseFrame();
    EXPECT_FALSE(r1.has_value());

    codec.AppendData(encoded.data() + 5, encoded.size() - 5);
    auto r2 = codec.TryParseFrame();
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->msg_type, FrameMessageType::Response);
    EXPECT_EQ(r2->payload, frame.payload);
}

TEST(StreamingFrameCodecTest, MultipleFrames) {
    Frame f1;
    f1.msg_type = FrameMessageType::Publish;
    f1.payload = {0x01};

    Frame f2;
    f2.msg_type = FrameMessageType::Subscribe;
    f2.payload = {0x02, 0x03};

    auto e1 = FrameCodec::Encode(f1);
    auto e2 = FrameCodec::Encode(f2);

    std::vector<uint8_t> buffer;
    buffer.insert(buffer.end(), e1.begin(), e1.end());
    buffer.insert(buffer.end(), e2.begin(), e2.end());

    StreamingFrameCodec codec;
    codec.AppendData(buffer.data(), buffer.size());

    auto r1 = codec.TryParseFrame();
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->msg_type, FrameMessageType::Publish);

    auto r2 = codec.TryParseFrame();
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->msg_type, FrameMessageType::Subscribe);

    auto r3 = codec.TryParseFrame();
    EXPECT_FALSE(r3.has_value());
}

TEST(StreamingFrameCodecTest, MagicSyncWithGarbagePrefix) {
    Frame frame;
    frame.msg_type = FrameMessageType::Ack;
    frame.payload = {0x11, 0x22};

    auto encoded = FrameCodec::Encode(frame);

    std::vector<uint8_t> buffer;
    // 添加垃圾前缀
    buffer.push_back(0x00);
    buffer.push_back(0xDE);
    buffer.push_back(0xAD);
    buffer.insert(buffer.end(), encoded.begin(), encoded.end());

    StreamingFrameCodec codec;
    codec.AppendData(buffer.data(), buffer.size());

    auto result = codec.TryParseFrame();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->msg_type, FrameMessageType::Ack);
    EXPECT_EQ(result->payload, frame.payload);
}

TEST(StreamingFrameCodecTest, MagicSyncWithInvalidVersion) {
    Frame frame;
    frame.msg_type = FrameMessageType::Ping;
    frame.payload = {0x99};

    auto encoded = FrameCodec::Encode(frame);

    std::vector<uint8_t> buffer;
    // Magic 正确但 version 无效的帧
    buffer.insert(buffer.end(), {0xDE, 0xAD, 0xBE, 0xEF, 0xFF, 0x01});
    // 真正的帧
    buffer.insert(buffer.end(), encoded.begin(), encoded.end());

    StreamingFrameCodec codec;
    codec.AppendData(buffer.data(), buffer.size());

    auto result = codec.TryParseFrame();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->msg_type, FrameMessageType::Ping);
    EXPECT_EQ(result->payload, frame.payload);
}

TEST(StreamingFrameCodecTest, ResetClearsState) {
    Frame frame;
    frame.msg_type = FrameMessageType::Push;
    frame.payload = {0x55};

    auto encoded = FrameCodec::Encode(frame);

    StreamingFrameCodec codec;
    codec.AppendData(encoded.data(), 5);
    EXPECT_GT(codec.GetBufferSize(), 0);

    codec.Reset();
    EXPECT_EQ(codec.GetBufferSize(), 0);

    // 重新追加完整数据应能正常解析
    codec.AppendData(encoded.data(), encoded.size());
    auto result = codec.TryParseFrame();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->msg_type, FrameMessageType::Push);
}

TEST(StreamingFrameCodecTest, EmptyDataNoCrash) {
    StreamingFrameCodec codec;
    codec.AppendData(nullptr, 0);
    auto result = codec.TryParseFrame();
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(codec.GetBufferSize(), 0);
}

TEST(StreamingFrameCodecTest, PartialMagicThenRest) {
    Frame frame;
    frame.msg_type = FrameMessageType::Pong;
    frame.payload = {};

    auto encoded = FrameCodec::Encode(frame);

    StreamingFrameCodec codec;
    // 先给 Magic 的前两个字节
    codec.AppendData(encoded.data(), 2);
    EXPECT_FALSE(codec.TryParseFrame().has_value());

    // 再给剩余部分
    codec.AppendData(encoded.data() + 2, encoded.size() - 2);
    auto result = codec.TryParseFrame();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->msg_type, FrameMessageType::Pong);
}
