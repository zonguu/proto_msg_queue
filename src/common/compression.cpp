#include "common/compression.h"

#include <zlib.h>
#include <cstring>

namespace pmqueue {

std::vector<uint8_t> Compress(const std::vector<uint8_t>& data) {
    if (data.empty()) {
        return {};
    }

    // zlib 压缩输出缓冲区大小上限
    uLongf compressed_size = compressBound(static_cast<uLong>(data.size()));
    std::vector<uint8_t> compressed;
    compressed.resize(compressed_size);

    int result = compress2(
        compressed.data(),
        &compressed_size,
        data.data(),
        static_cast<uLong>(data.size()),
        Z_DEFAULT_COMPRESSION
    );

    if (result != Z_OK) {
        return {};
    }

    compressed.resize(compressed_size);
    return compressed;
}

std::vector<uint8_t> Decompress(const std::vector<uint8_t>& data) {
    if (data.empty()) {
        return {};
    }

    // 预估解压后大小：先尝试 4 倍，若不够则逐步扩大
    size_t estimate = data.size() * 4;
    if (estimate < 1024) estimate = 1024;

    std::vector<uint8_t> decompressed;

    for (int attempt = 0; attempt < 10; ++attempt) {
        decompressed.resize(estimate);
        uLongf decompressed_size = static_cast<uLongf>(estimate);

        int result = uncompress(
            decompressed.data(),
            &decompressed_size,
            data.data(),
            static_cast<uLong>(data.size())
        );

        if (result == Z_OK) {
            decompressed.resize(decompressed_size);
            return decompressed;
        }

        if (result == Z_BUF_ERROR) {
            estimate *= 2;
            continue;
        }

        // 其他错误
        return {};
    }

    return {};
}

} // namespace pmqueue
