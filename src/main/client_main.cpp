#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include "network/tcp_client.h"
#include "protocol/frame_codec.h"
#include "msg_queue.pb.h"
#include "common/compression.h"

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " <host> <port> <command> [args...]" << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  publish <topic> <message> [ttl_ms]       - Publish a message" << std::endl;
    std::cout << "  batch_publish <topic> <msg1> [msg2]...   - Batch publish messages" << std::endl;
    std::cout << "  subscribe <topic> <sub_id> [group_id]    - Subscribe to a topic" << std::endl;
    std::cout << "  unsubscribe <topic> <sub_id> [group_id]  - Unsubscribe from a topic" << std::endl;
    std::cout << "  pull <topic> <sub_id> [group_id] [max]   - Pull messages" << std::endl;
    std::cout << "  ack <topic> <sub_id> <msg_id> [group]    - Ack a message" << std::endl;
    std::cout << "  admin <command> [topic]                    - Admin command" << std::endl;
    std::cout << "    Commands: list_topics, get_topic_info, delete_topic, get_stats," << std::endl;
    std::cout << "              get_connections, cleanup_topic" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        PrintUsage(argv[0]);
        return 1;
    }

    std::string host = argv[1];
    uint16_t port = static_cast<uint16_t>(std::atoi(argv[2]));
    std::string cmd = argv[3];

    pmqueue::TcpClient client;
    client.SetFrameHandler([](const pmqueue::Frame& frame) {
        if (frame.msg_type == pmqueue::FrameMessageType::Response) {
            pmqueue::Response resp;
            if (resp.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                std::cout << "Response: success=" << resp.success()
                          << " msg_id=" << resp.message_id();
                if (!resp.error_msg().empty()) {
                    std::cout << " error=" << resp.error_msg();
                }
                std::cout << std::endl;
            }
        } else if (frame.msg_type == pmqueue::FrameMessageType::Push) {
            pmqueue::PushMessage push;
            if (push.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                std::string payload = push.payload();
                // 如果消息被压缩，解压后展示
                if (push.compressed()) {
                    std::vector<uint8_t> compressed(push.payload().begin(), push.payload().end());
                    auto decompressed = pmqueue::Decompress(compressed);
                    if (!decompressed.empty()) {
                        payload = std::string(decompressed.begin(), decompressed.end());
                    } else {
                        payload = "[decompress_failed]";
                    }
                }
                std::cout << "[PUSH] topic=" << push.topic()
                          << " msg_id=" << push.message_id()
                          << " retry=" << push.retry_count()
                          << " compressed=" << push.compressed()
                          << " payload=" << payload << std::endl;
            }
        } else if (frame.msg_type == pmqueue::FrameMessageType::Admin) {
            pmqueue::AdminResponse resp;
            if (resp.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                std::cout << "[ADMIN] success=" << resp.success();
                if (!resp.error_msg().empty()) {
                    std::cout << " error=" << resp.error_msg();
                }
                if (!resp.json_result().empty()) {
                    std::cout << " result=" << resp.json_result();
                }
                std::cout << std::endl;
            }
        } else if (frame.msg_type == pmqueue::FrameMessageType::BatchPush) {
            pmqueue::BatchPushMessage batch;
            if (batch.ParseFromArray(frame.payload.data(), static_cast<int>(frame.payload.size()))) {
                for (const auto& push : batch.messages()) {
                    std::string payload = push.payload();
                    if (push.compressed()) {
                        std::vector<uint8_t> compressed(push.payload().begin(), push.payload().end());
                        auto decompressed = pmqueue::Decompress(compressed);
                        if (!decompressed.empty()) {
                            payload = std::string(decompressed.begin(), decompressed.end());
                        } else {
                            payload = "[decompress_failed]";
                        }
                    }
                    std::cout << "[BATCH_PUSH] topic=" << push.topic()
                              << " msg_id=" << push.message_id()
                              << " retry=" << push.retry_count()
                              << " compressed=" << push.compressed()
                              << " payload=" << payload << std::endl;
                }
            }
        }
    });

    if (!client.Connect(host, port)) {
        std::cerr << "Failed to connect to " << host << ":" << port << std::endl;
        return 1;
    }

    std::cout << "Connected to " << host << ":" << port << std::endl;

    pmqueue::Frame frame;
    std::string data;

    if (cmd == "publish" && argc >= 6) {
        pmqueue::PublishRequest req;
        req.set_topic(argv[4]);
        req.set_payload(argv[5]);
        if (argc >= 7) req.set_ttl_ms(static_cast<uint32_t>(std::atoi(argv[6])));
        req.set_producer_id(client.GetProducerId());
        req.set_sequence_id(client.GetNextSequenceId());
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Publish;
    } else if (cmd == "batch_publish" && argc >= 6) {
        pmqueue::BatchPublishRequest batch_req;
        for (int i = 5; i < argc; ++i) {
            auto* msg = batch_req.add_messages();
            msg->set_topic(argv[4]);
            msg->set_payload(argv[i]);
            msg->set_producer_id(client.GetProducerId());
            msg->set_sequence_id(client.GetNextSequenceId());
        }
        batch_req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::BatchPublish;
    } else if (cmd == "subscribe" && argc >= 6) {
        pmqueue::SubscribeRequest req;
        req.set_topic(argv[4]);
        req.set_subscriber_id(argv[5]);
        if (argc >= 7) req.set_group_id(argv[6]);
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Subscribe;
    } else if (cmd == "unsubscribe" && argc >= 6) {
        pmqueue::UnsubscribeRequest req;
        req.set_topic(argv[4]);
        req.set_subscriber_id(argv[5]);
        if (argc >= 7) req.set_group_id(argv[6]);
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Unsubscribe;
    } else if (cmd == "pull" && argc >= 6) {
        pmqueue::PullRequest req;
        req.set_topic(argv[4]);
        req.set_subscriber_id(argv[5]);
        if (argc >= 7) req.set_group_id(argv[6]);
        req.set_max_messages(argc >= 8 ? std::atoi(argv[7]) : 10);
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Pull;
    } else if (cmd == "ack" && argc >= 7) {
        pmqueue::AckRequest req;
        req.set_topic(argv[4]);
        req.set_subscriber_id(argv[5]);
        req.set_message_id(std::stoull(argv[6]));
        if (argc >= 8) req.set_group_id(argv[7]);
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Ack;
    } else if (cmd == "admin" && argc >= 5) {
        pmqueue::AdminRequest req;
        std::string admin_cmd = argv[4];
        if (admin_cmd == "list_topics") {
            req.set_command(pmqueue::ADMIN_LIST_TOPICS);
        } else if (admin_cmd == "get_topic_info") {
            req.set_command(pmqueue::ADMIN_GET_TOPIC_INFO);
            if (argc >= 6) req.set_topic(argv[5]);
        } else if (admin_cmd == "delete_topic") {
            req.set_command(pmqueue::ADMIN_DELETE_TOPIC);
            if (argc >= 6) req.set_topic(argv[5]);
        } else if (admin_cmd == "get_stats") {
            req.set_command(pmqueue::ADMIN_GET_STATS);
        } else if (admin_cmd == "get_connections") {
            req.set_command(pmqueue::ADMIN_GET_CONNECTIONS);
        } else if (admin_cmd == "cleanup_topic") {
            req.set_command(pmqueue::ADMIN_CLEANUP_TOPIC);
            if (argc >= 6) req.set_topic(argv[5]);
        } else {
            std::cerr << "Unknown admin command: " << admin_cmd << std::endl;
            return 1;
        }
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Admin;
    } else {
        PrintUsage(argv[0]);
        return 1;
    }

    frame.payload.assign(data.begin(), data.end());
    client.SendFrame(frame);

    // 等待响应
    std::this_thread::sleep_for(std::chrono::seconds(2));

    client.Disconnect();
    return 0;
}
