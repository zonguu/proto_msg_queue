#include <iostream>
#include <csignal>
#include <memory>

#include "mq/broker.h"
#include "storage/memory_message_store.h"

std::unique_ptr<pmqueue::Broker> g_broker;

void SignalHandler(int sig) {
    std::cout << "\nReceived signal " << sig << ", shutting down..." << std::endl;
    if (g_broker) {
        g_broker->Stop();
    }
}

int main(int argc, char* argv[]) {
    uint16_t port = 9090;
    if (argc > 1) {
        port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    auto store = std::make_unique<pmqueue::MemoryMessageStore>();
    g_broker = std::make_unique<pmqueue::Broker>(std::move(store), port);

    if (!g_broker->Start()) {
        std::cerr << "Failed to start broker on port " << port << std::endl;
        return 1;
    }

    std::cout << "PMQ Broker started on port " << port << std::endl;
    std::cout << "Press Ctrl+C to stop." << std::endl;

    while (g_broker->IsRunning()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "Broker stopped." << std::endl;
    return 0;
}
