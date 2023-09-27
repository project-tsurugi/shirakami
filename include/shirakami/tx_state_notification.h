#pragma once

#include <cstdint>
#include <functional>

#include "result_info.h"
#include "scheme.h"

namespace shirakami {

/**
 * @brief durability marker type
 * @details monotonic (among durability callback invocations) marker to indicate 
 * how far durability processing completed
 */
using durability_marker_type = std::uint64_t;

/**
 * @brief commit callback type
 * @details callback to receive commit result.
 */
using commit_callback_type =
        std::function<void(Status, reason_code, durability_marker_type)>;

/**
 * @brief durability callback type
 * @details callback to receive durability marker value
 */
using durability_callback_type = std::function<void(durability_marker_type)>;

/**
 * @brief register durability callback
 * @details register the durability callback function for shirakami.
 * Caller must ensure the callback `cb` is kept safely callable until 
 * shirakami::fin(). By calling the function multiple-times, multiple callbacks 
 * can be registered for a single shirakami interface. When there are multiple 
 * callbacks registered, the order of callback invocation is undefined.
 * When shirakami::fin() is called, the callback object passed as `cb`
 * parameter is destroyed.
 * It is not thread safe.
 * @param cb the callback function invoked on durability status change
 * @return Status::OK if function is successful
 * @return any error otherwise
 */

extern Status register_durability_callback(durability_callback_type cb);

} // namespace shirakami