#include <gtest/gtest.h>
#include <vector>
#include <string>

#include "common/compression.h"

using namespace pmqueue;

TEST(CompressionTest, BasicCompressDecompress) {
    std::vector<uint8_t> original;
    std::string text = "Hello, World! This is a test message for compression. "
                       "It needs to be long enough to benefit from zlib compression. "
                       "Repeating patterns help compression algorithms achieve better ratios. "
                       "Hello, World! This is a test message for compression. "
                       "It needs to be long enough to benefit from zlib compression. "
                       "Repeating patterns help compression algorithms achieve better ratios.";
    original.assign(text.begin(), text.end());

    auto compressed = Compress(original);
    EXPECT_FALSE(compressed.empty());
    // 有重复内容，压缩后应更小
    EXPECT_LT(compressed.size(), original.size());

    auto decompressed = Decompress(compressed);
    EXPECT_EQ(decompressed.size(), original.size());
    EXPECT_EQ(decompressed, original);
}

TEST(CompressionTest, EmptyData) {
    std::vector<uint8_t> empty;
    auto compressed = Compress(empty);
    EXPECT_TRUE(compressed.empty());

    auto decompressed = Decompress(empty);
    EXPECT_TRUE(decompressed.empty());
}

TEST(CompressionTest, ShortData) {
    std::vector<uint8_t> short_data = {0x01, 0x02, 0x03};
    auto compressed = Compress(short_data);
    // 短数据压缩后可能比原数据大，但函数应正常工作
    EXPECT_FALSE(compressed.empty());

    auto decompressed = Decompress(compressed);
    EXPECT_EQ(decompressed, short_data);
}

TEST(CompressionTest, LargeData) {
    std::vector<uint8_t> large(10000, 'A');
    auto compressed = Compress(large);
    EXPECT_FALSE(compressed.empty());
    EXPECT_LT(compressed.size(), large.size());

    auto decompressed = Decompress(compressed);
    EXPECT_EQ(decompressed, large);
}
