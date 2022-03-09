
#pragma once

#include <atomic>

#include "concurrency_control/wp/include/session.h"

namespace shirakami {

/**
 * @brief Whether init function was called in this system.
 */
inline std::atomic<bool> initialized_{false};

/**
 * @brief getter of @a intialized_.
 */
[[maybe_unused]] static bool get_initialized() { return initialized_.load(std::memory_order_acquire); }

/**
 * @brief setter of @a intialized_.
 */
[[maybe_unused]] static void set_initialized(bool tf) { initialized_.store(tf, std::memory_order_release); }

[[maybe_unused]] extern Status check_before_write_ops(session* ti, Storage st, OP_TYPE op);

} // namespace shirakami