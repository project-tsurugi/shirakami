#pragma once

#include <atomic>
#include <cstdlib>
#include <iostream>

#include "shirakami/logging.h"

#include "glog/logging.h"

namespace shirakami {

inline std::atomic<bool> is_debug_mode_{false};

// Shirakami Debug Log
#define SDL LOG_IF(INFO, get_is_debug_mode())

[[maybe_unused]] static bool get_is_debug_mode() {
    return is_debug_mode_.load(std::memory_order_acquire);
}

[[maybe_unused]] static void set_is_debug_mode(bool const tf) {
    is_debug_mode_.store(tf, std::memory_order_release);
}

} // namespace shirakami

namespace shirakami::logging {

#define shirakami_log_entry                                                    \
    VLOG(log_trace) << std::boolalpha << log_location_prefix << "-->" // NOLINT
#define shirakami_log_exit                                                     \
    VLOG(log_trace) << std::boolalpha << log_location_prefix << "<--" // NOLINT
#define shirakami_ex_log_entry                                                 \
    VLOG(log_ex_trace) << std::boolalpha << log_location_prefix                \
                       << "-->" // NOLINT
#define shirakami_ex_log_exit                                                  \
    VLOG(log_ex_trace) << std::boolalpha << log_location_prefix                \
                       << "<--" // NOLINT

// receive string_view only
#define shirakami_binstring(arg)                                               \
    " " #arg "(len=" << (arg).size() << "):\"" << binary_printer((arg))        \
                     << "\"" //NOLINT

inline std::atomic<bool> enable_logging_detail_info_{false};

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