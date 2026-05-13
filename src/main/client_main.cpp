#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include "network/tcp_client.h"
#include "protocol/frame_codec.h"
#include "msg_queue.pb.h"

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " <host> <port> <command> [args...]" << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  publish <topic> <message>     - Publish a message" << std::endl;
    std::cout << "  subscribe <topic> <sub_id>    - Subscribe to a topic" << std::endl;
    std::cout << "  unsubscribe <topic> <sub_id>  - Unsubscribe from a topic" << std::endl;
    std::cout << "  pull <topic> <sub_id> [max]   - Pull messages" << std::endl;
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
                std::cout << "[PUSH] topic=" << push.topic()
                          << " msg_id=" << push.message_id()
                          << " payload=" << push.payload() << std::endl;
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
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Publish;
    } else if (cmd == "subscribe" && argc >= 6) {
        pmqueue::SubscribeRequest req;
        req.set_topic(argv[4]);
        req.set_subscriber_id(argv[5]);
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Subscribe;
    } else if (cmd == "unsubscribe" && argc >= 6) {
        pmqueue::UnsubscribeRequest req;
        req.set_topic(argv[4]);
        req.set_subscriber_id(argv[5]);
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Unsubscribe;
    } else if (cmd == "pull" && argc >= 6) {
        pmqueue::PullRequest req;
        req.set_topic(argv[4]);
        req.set_subscriber_id(argv[5]);
        req.set_max_messages(argc >= 7 ? std::atoi(argv[6]) : 10);
        req.SerializeToString(&data);
        frame.msg_type = pmqueue::FrameMessageType::Pull;
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
