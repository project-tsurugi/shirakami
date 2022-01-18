/**
 * @file concurrency_control/silo/interface/include/helper.h
 */

#pragma once

#include <atomic>

#include "concurrency_control/silo/include/session.h"

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
 * @brief read record by using dest given by caller and store read info to res
 * given by caller.
 * @pre the dest wasn't already read by itself.
 * @param [out] res it is stored read info.
 * @param [in] dest read record pointed by this dest.
 * @param [in] read_value whether read the value of record.
 * @return WARN_CONCURRENT_DELETE No corresponding record in masstree. If you
 * have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::OK, it was ended correctly.
 * but it isn't committed yet.
 */
Status read_record(Record &res, const Record* dest, bool read_value = true);  // NOLINT

/**
 * @brief setter of @a intialized_.
 */
[[maybe_unused]] static void set_initialized(bool tf) { initialized_.store(tf, std::memory_order_release); }

}  // namespace shirakami
