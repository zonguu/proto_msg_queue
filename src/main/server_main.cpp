#include <iostream>
#include <csignal>
#include <memory>
#include <string>

#include "mq/broker.h"
#include "storage/memory_message_store.h"
#include "common/config.h"

std::unique_ptr<pmqueue::Broker> g_broker;

void SignalHandler(int sig) {
    std::cout << "\nReceived signal " << sig << ", shutting down..." << std::endl;
    if (g_broker) {
        g_broker->Stop();
    }
}

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --port <port>         Server port (default: 9090)" << std::endl;
    std::cout << "  --config <file>       Load config from JSON file" << std::endl;
    std::cout << "  --save-config <file>  Save default config to JSON file and exit" << std::endl;
    std::cout << "  --help                Show this help" << std::endl;
}

int main(int argc, char* argv[]) {
    pmqueue::BrokerConfig config;
    std::string config_file;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (arg == "--config" && i + 1 < argc) {
            config_file = argv[++i];
        } else if (arg == "--save-config" && i + 1 < argc) {
            pmqueue::BrokerConfig default_cfg;
            if (default_cfg.SaveToFile(argv[++i])) {
                std::cout << "Default config saved to " << argv[i] << std::endl;
            } else {
                std::cerr << "Failed to save config to " << argv[i] << std::endl;
                return 1;
            }
            return 0;
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        }
    }

    if (!config_file.empty()) {
        if (!config.LoadFromFile(config_file)) {
            std::cerr << "Failed to load config from " << config_file << std::endl;
            return 1;
        }
        std::cout << "Loaded config from " << config_file << std::endl;
    }

    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    auto store = std::make_unique<pmqueue::MemoryMessageStore>(
        config.default_ring_buffer_size,
        config.max_retry_count,
        config.pending_timeout_ms,
        config.retry_interval_ms,
        config.expiration_check_interval_ms
    );
    g_broker = std::make_unique<pmqueue::Broker>(std::move(store), config);

    if (!g_broker->Start()) {
        std::cerr << "Failed to start broker on port " << config.port << std::endl;
        return 1;
    }

    std::cout << "PMQ Broker started on port " << config.port << std::endl;
    std::cout << "Features: batch=" << (config.batch_publish_enabled ? "on" : "off")
              << " heartbeat=" << (config.heartbeat_enabled ? "on" : "off")
              << " rate_limit=" << (config.rate_limit_enabled ? "on" : "off")
              << " ttl=" << (config.ttl_enabled ? "on" : "off")
              << " compression=" << (config.compression_enabled ? "on" : "off")
              << std::endl;
    std::cout << "Press Ctrl+C to stop." << std::endl;

    while (g_broker->IsRunning()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "Broker stopped." << std::endl;
    return 0;
}
