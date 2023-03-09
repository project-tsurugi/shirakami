#pragma once

#include <atomic>
#include <cstdlib>

#include "shirakami/logging.h"

#include "glog/logging.h"

namespace shirakami::logging {

inline std::atomic<bool> enable_logging_detail_info_{false};

[[maybe_unused]] static inline void dvlog(std::string_view const str) {
    DVLOG(log_trace) << str;
}

[[maybe_unused]] static bool get_enable_logging_detail_info() {
    return enable_logging_detail_info_.load(std::memory_order_acquire);
}

[[maybe_unused]] static void set_enable_logging_detail_info(bool const tf) {
    enable_logging_detail_info_.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void init(bool const enable_logging_detail_info) {
    set_enable_logging_detail_info(enable_logging_detail_info);

    // work around
    auto* sdi = std::getenv("SHIRAKAMI_DETAIL_INFO");
    if (sdi != nullptr) { set_enable_logging_detail_info(true); }
}

} // namespace shirakami::logging