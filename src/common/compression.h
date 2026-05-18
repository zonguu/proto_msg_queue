#pragma once

#include <cstdint>
#include <vector>

namespace pmqueue {

/**
 * @brief 使用 zlib deflate 压缩数据
 * @param data 原始数据
 * @return 压缩后的数据（若压缩失败或数据太小，返回空）
 */
std::vector<uint8_t> Compress(const std::vector<uint8_t>& data);

/**
 * @brief 使用 zlib inflate 解压数据
 * @param data 压缩数据
 * @return 解压后的数据（若失败返回空）
 */
std::vector<uint8_t> Decompress(const std::vector<uint8_t>& data);

} // namespace pmqueue
