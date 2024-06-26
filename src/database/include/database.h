#pragma once

#include "shirakami/interface.h"

namespace shirakami {

inline std::atomic<bool> is_shutdowning_{false}; // NOLINT

inline database_options used_database_options_{}; // NOLINT

[[maybe_unused]] static database_options get_used_database_options() {
    return used_database_options_;
}

[[maybe_unused]] static bool get_is_shutdowning() {
    return is_shutdowning_.load(std::memory_order_acquire);
}

[[maybe_unused]] static void
set_used_database_options(database_options const& dos) {
    used_database_options_ = dos;
}

[[maybe_unused]] static void set_is_shutdowning(bool tf) {
    is_shutdowning_.store(tf, std::memory_order_release);
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
