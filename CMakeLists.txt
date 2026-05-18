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
    src/ring_buffer/spsc_ring_buffer.cpp
    src/protocol/frame_codec.cpp
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
)

# === 可执行文件 ===
add_executable(server_main src/main/server_main.cpp)
target_link_libraries(server_main PRIVATE pmqueue_core)

add_executable(client_main src/main/client_main.cpp)
target_link_libraries(client_main PRIVATE pmqueue_core)

# === 测试 ===
enable_testing()

add_executable(test_ring_buffer test/test_ring_buffer.cpp)
target_link_libraries(test_ring_buffer PRIVATE pmqueue_core GTest::gtest_main)

add_executable(test_frame_codec test/test_frame_codec.cpp)
target_link_libraries(test_frame_codec PRIVATE pmqueue_core GTest::gtest_main)

add_executable(test_message_store test/test_message_store.cpp)
target_link_libraries(test_message_store PRIVATE pmqueue_core GTest::gtest_main)

add_executable(test_topic_manager test/test_topic_manager.cpp)
target_link_libraries(test_topic_manager PRIVATE pmqueue_core GTest::gtest_main)

add_executable(test_integration test/test_integration.cpp)
target_link_libraries(test_integration PRIVATE pmqueue_core GTest::gtest_main)

add_executable(test_batch_publish test/test_batch_publish.cpp)
target_link_libraries(test_batch_publish PRIVATE pmqueue_core GTest::gtest_main)

add_executable(test_heartbeat test/test_heartbeat.cpp)
target_link_libraries(test_heartbeat PRIVATE pmqueue_core GTest::gtest_main)

add_executable(test_rate_limiter test/test_rate_limiter.cpp)
target_link_libraries(test_rate_limiter PRIVATE pmqueue_core GTest::gtest_main)

add_executable(test_ttl test/test_ttl.cpp)
target_link_libraries(test_ttl PRIVATE pmqueue_core GTest::gtest_main)

# 注册测试
add_test(NAME RingBufferTest COMMAND test_ring_buffer)
add_test(NAME FrameCodecTest COMMAND test_frame_codec)
add_test(NAME MessageStoreTest COMMAND test_message_store)
add_test(NAME TopicManagerTest COMMAND test_topic_manager)
add_test(NAME IntegrationTest COMMAND test_integration)
add_test(NAME BatchPublishTest COMMAND test_batch_publish)
add_test(NAME HeartbeatTest COMMAND test_heartbeat)
add_test(NAME RateLimiterTest COMMAND test_rate_limiter)
add_test(NAME TtlTest COMMAND test_ttl)
