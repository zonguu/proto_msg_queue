#include <gtest/gtest.h>

#include "protocol/frame_codec.h"
#include "protocol/frame_protocol.h"

using namespace pmqueue;

TEST(FrameCodecTest, EncodeDecode) {
    Frame frame;
    frame.msg_type = FrameMessageType::Publish;
    frame.payload = {0x01, 0x02, 0x03, 0x04, 0x05};

    auto encoded = FrameCodec::Encode(frame);
    EXPECT_EQ(encoded.size(), kFrameHeaderSize + 5);

    // 验证 Magic
    EXPECT_EQ(encoded[0], 0xDE);
    EXPECT_EQ(encoded[1], 0xAD);
    EXPECT_EQ(encoded[2], 0xBE);
    EXPECT_EQ(encoded[3], 0xEF);

    // 验证 Version
    EXPECT_EQ(encoded[4], 0x01);

    // 验证 MsgType
    EXPECT_EQ(encoded[5], static_cast<uint8_t>(FrameMessageType::Publish));

    // 验证 Length (big-endian, 5)
    EXPECT_EQ(encoded[6], 0x00);
    EXPECT_EQ(encoded[7], 0x00);
    EXPECT_EQ(encoded[8], 0x00);
    EXPECT_EQ(encoded[9], 0x05);

    // 验证 Payload
    EXPECT_EQ(encoded[10], 0x01);
    EXPECT_EQ(encoded[11], 0x02);
    EXPECT_EQ(encoded[12], 0x03);
    EXPECT_EQ(encoded[13], 0x04);
    EXPECT_EQ(encoded[14], 0x05);

    // 解码
    size_t consumed = 0;
    auto decoded = FrameCodec::TryDecode(encoded, consumed);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(consumed, encoded.size());
    EXPECT_EQ(decoded->magic, kFrameMagic);
    EXPECT_EQ(decoded->version, kFrameVersion);
    EXPECT_EQ(decoded->msg_type, FrameMessageType::Publish);
    EXPECT_EQ(decoded->payload, frame.payload);
}

TEST(FrameCodecTest, IncompleteFrame) {
    std::vector<uint8_t> buffer = {0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x01};
    size_t consumed = 0;
    auto result = FrameCodec::TryDecode(buffer, consumed);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(consumed, 0);
}

TEST(FrameCodecTest, IncompletePayload) {
    std::vector<uint8_t> buffer = {
        0xDE, 0xAD, 0xBE, 0xEF, // Magic
        0x01,                     // Version
        0x01,                     // MsgType
        0x00, 0x00, 0x00, 0x10   // Length = 16, but only 2 bytes payload
    };
    buffer.push_back(0xAA);
    buffer.push_back(0xBB);

    size_t consumed = 0;
    auto result = FrameCodec::TryDecode(buffer, consumed);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(consumed, 0);
}

TEST(FrameCodecTest, HasCompleteFrame) {
    Frame frame;
    frame.msg_type = FrameMessageType::Response;
    frame.payload = {0xAA, 0xBB};

    auto encoded = FrameCodec::Encode(frame);
    EXPECT_TRUE(FrameCodec::HasCompleteFrame(encoded));

    std::vector<uint8_t> incomplete(encoded.begin(), encoded.begin() + 5);
    EXPECT_FALSE(FrameCodec::HasCompleteFrame(incomplete));
}

TEST(FrameCodecTest, MultipleFramesInBuffer) {
    Frame frame1;
    frame1.msg_type = FrameMessageType::Publish;
    frame1.payload = {0x01};

    Frame frame2;
    frame2.msg_type = FrameMessageType::Subscribe;
    frame2.payload = {0x02, 0x03};

    auto encoded1 = FrameCodec::Encode(frame1);
    auto encoded2 = FrameCodec::Encode(frame2);

    std::vector<uint8_t> buffer;
    buffer.insert(buffer.end(), encoded1.begin(), encoded1.end());
    buffer.insert(buffer.end(), encoded2.begin(), encoded2.end());

    size_t consumed1 = 0;
    auto decoded1 = FrameCodec::TryDecode(buffer, consumed1);
    ASSERT_TRUE(decoded1.has_value());
    EXPECT_EQ(decoded1->msg_type, FrameMessageType::Publish);

    std::vector<uint8_t> remaining(buffer.begin() + consumed1, buffer.end());
    size_t consumed2 = 0;
    auto decoded2 = FrameCodec::TryDecode(remaining, consumed2);
    ASSERT_TRUE(decoded2.has_value());
    EXPECT_EQ(decoded2->msg_type, FrameMessageType::Subscribe);
}

TEST(FrameCodecTest, PeekPayloadLength) {
    Frame frame;
    frame.msg_type = FrameMessageType::Ack;
    frame.payload = std::vector<uint8_t>(100, 0xAB);

    auto encoded = FrameCodec::Encode(frame);
    auto len = FrameCodec::PeekPayloadLength(encoded);
    ASSERT_TRUE(len.has_value());
    EXPECT_EQ(len.value(), 100);

    std::vector<uint8_t> incomplete = {0xDE, 0xAD};
    auto no_len = FrameCodec::PeekPayloadLength(incomplete);
    EXPECT_FALSE(no_len.has_value());
}

TEST(FrameCodecTest, LargePayload) {
    Frame frame;
    frame.msg_type = FrameMessageType::Push;
    frame.payload = std::vector<uint8_t>(1024 * 1024, 0xCD); // 1MB

    auto encoded = FrameCodec::Encode(frame);
    size_t consumed = 0;
    auto decoded = FrameCodec::TryDecode(encoded, consumed);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->payload.size(), 1024 * 1024);
}
