#pragma once

#include "shirakami/interface.h"

namespace shirakami {

inline log_event_callback log_event_callback_; // NOLINT

[[maybe_unused]] static void clear_log_event_callback() {
    log_event_callback f;
    log_event_callback_ = f;
}

[[maybe_unused]] static log_event_callback get_log_event_callback() {
    return log_event_callback_;
}

[[maybe_unused]] static void
set_log_event_callback(log_event_callback const& callback) {
    log_event_callback_ = callback;
}

/**
 * @brief Delete the all records in all tables.
 * @pre This function is called by a single thread and doesn't allow concurrent 
 * processing by other threads. 
 * This is not transactional operation.
 * @return Status::OK success
 */
[[maybe_unused]] extern Status delete_all_records(); // NOLINT

} // namespace shirakami