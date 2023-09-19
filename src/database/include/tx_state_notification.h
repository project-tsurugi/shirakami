#pragma once

#include <mutex>
#include <vector>

#include "shirakami/tx_state_notification.h"

namespace shirakami {

inline std::vector<durability_callback_type> durability_callbacks{}; // NOLINT

inline std::mutex mtx_durability_callbacks{};

/**
 * @brief Registration of durability_callback.
 * @details At the time of registering the callback function, call the callback 
 * function once at the persistence boundary in shirakami.
*/
extern void add_durability_callbacks(durability_callback_type& dc);

extern void call_durability_callbacks(durability_marker_type dm);

extern void clear_durability_callbacks();

[[maybe_unused]] static std::vector<durability_callback_type>&
get_durability_callbacks() {
    return durability_callbacks;
}

[[maybe_unused]] static std::mutex& get_mtx_durability_callbacks() {
    return mtx_durability_callbacks;
}

} // namespace shirakami