#pragma once

#include <cstdint>

namespace shirakami {

static constexpr std::string_view log_location_prefix = "/:shirakami ";

static constexpr std::string_view log_location_prefix_config =
        "/:shirakami:config: ";

static constexpr std::string_view log_location_prefix_detail_info =
        "/:shirakami:detail_info: ";

static constexpr std::string_view log_location_prefix_timing_event =
        "/:shirakami:timing:"; // +<event_name>

/**
 * @brief logging level constant for errors
 */
static constexpr std::int32_t log_error = 10;

/**
 * @brief logging level constant for warnings
 */
static constexpr std::int32_t log_warning = 20;

/**
 * @brief logging level constant for information
 */
static constexpr std::int32_t log_info = 30;

/**
 * @brief logging level constant for debug timing event information.
 * 
 */
static constexpr std::int32_t log_debug_timing_event = 35;

/**
 * @brief logging level constant for debug information
 */
static constexpr std::int32_t log_debug = 40;

/**
 * @brief logging level constant for traces
 */
static constexpr std::int32_t log_trace = 50;

} // namespace shirakami