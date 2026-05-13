#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/types.h"

namespace pmqueue {

struct SubscriberInfo {
    SubscriberId id;
    ConnectionId conn_id;
    MessageCallback callback;
};

struct TopicInfo {
    std::string name;
    uint64_t create_time;
    uint64_t message_count;
};

} // namespace pmqueue
