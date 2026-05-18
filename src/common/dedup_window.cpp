#include "common/dedup_window.h"

#include <algorithm>
#include <vector>

namespace pmqueue {

DedupWindow::DedupWindow(size_t max_producers)
    : max_producers_(max_producers) {}

bool DedupWindow::IsDuplicate(const std::string& producer_id, uint64_t sequence_id) {
    if (producer_id.empty()) {
        return false; // 无 producer_id 时不进行去重（兼容旧客户端）
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = producers_.find(producer_id);
    if (it == producers_.end()) {
        return false;
    }
    return sequence_id <= it->second.max_sequence;
}

void DedupWindow::MarkProcessed(const std::string& producer_id, uint64_t sequence_id) {
    if (producer_id.empty()) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto& state = producers_[producer_id];
    if (sequence_id > state.max_sequence) {
        state.max_sequence = sequence_id;
    }
    state.last_active = std::chrono::steady_clock::now();

    // 注意：不在这里触发清理，由 Broker 后台定期调用 CleanupInactive
}

void DedupWindow::RemoveProducer(const std::string& producer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    producers_.erase(producer_id);
}

void DedupWindow::CleanupInactive(size_t max_inactive) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (producers_.size() <= max_inactive) {
        return;
    }

    // 按最后活跃时间排序，移除最老的
    std::vector<std::pair<std::string, std::chrono::steady_clock::time_point>> sorted;
    sorted.reserve(producers_.size());
    for (const auto& [id, state] : producers_) {
        sorted.emplace_back(id, state.last_active);
    }

    std::sort(sorted.begin(), sorted.end(),
        [](const auto& a, const auto& b) {
            return a.second < b.second;
        });

    size_t to_remove = producers_.size() - max_inactive;
    for (size_t i = 0; i < to_remove; ++i) {
        producers_.erase(sorted[i].first);
    }
}

size_t DedupWindow::GetProducerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return producers_.size();
}

} // namespace pmqueue
