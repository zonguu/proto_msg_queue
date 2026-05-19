cmake_minimum_required(VERSION 3.16)
project(proto_msg_queue VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/output/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/output/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/output/lib)

# 包含公共 CMake 配置
include(cmake/common.cmake)

# 查找依赖
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(GTest REQUIRED)
find_package(ZLIB REQUIRED)

# Protobuf 生成
set(PROTO_SRC ${CMAKE_SOURCE_DIR}/src/proto/msg_queue.proto)
protobuf_generate_cpp(PROTO_CPP_SOURCES PROTO_CPP_HEADERS ${PROTO_SRC})

# 公共 include 目录
include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}
    ${CMAKE_BINARY_DIR}/src/proto
    ${Protobuf_INCLUDE_DIRS}
)

# === 核心库 ===
add_library(pmqueue_core STATIC
    ${PROTO_CPP_SOURCES}
    ${PROTO_CPP_HEADERS}
    src/common/config.cpp
    src/common/compression.cpp
    src/common/dedup_window.cpp
    src/ring_buffer/spsc_ring_buffer.cpp
    src/protocol/frame_codec.cpp
    src/protocol/streaming_frame_codec.cpp
    src/storage/memory_message_store.cpp
    src/network/tcp_server.cpp
    src/network/tcp_client.cpp
    src/network/connection.cpp
    src/mq/topic_manager.cpp
    src/mq/broker.cpp
)

target_link_libraries(pmqueue_core PUBLIC
    Threads::Threads
    ${Protobuf_LIBRARIES}
    ZLIB::ZLIB
)

# === 可执行文件 ===
add_executable(server_main src/main/server_main.cpp)
target_link_libraries(server_main PRIVATE pmqueue_core)

add_executable(client_main src/main/client_main.cpp)
target_link_libraries(client_main PRIVATE pmqueue_core)

# === 测试（合并为单个 bin）===
enable_testing()

add_executable(test_all
    test/test_util.cpp
    test/test_ring_buffer.cpp
    test/test_frame_codec.cpp
    test/test_message_store.cpp
    test/test_topic_manager.cpp
    test/test_integration.cpp
    test/test_batch_publish.cpp
    test/test_heartbeat.cpp
    test/test_rate_limiter.cpp
    test/test_ttl.cpp
    test/test_compression.cpp
    test/test_config.cpp
    test/test_zero_copy.cpp
    test/test_dedup.cpp
    test/test_streaming_codec.cpp
    test/test_admin.cpp
)

target_link_libraries(test_all PRIVATE pmqueue_core GTest::gtest_main)

# 注册测试
add_test(NAME AllTests COMMAND test_all)
