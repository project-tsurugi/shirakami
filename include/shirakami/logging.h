#pragma once

#include <cstdint>

#include "binary_printer.h"

namespace shirakami {

static constexpr std::string_view log_location_prefix = "/:shirakami ";

static constexpr std::string_view log_location_prefix_config =
        "/:shirakami:config: ";

static constexpr std::string_view log_location_prefix_detail_info =
        "/:shirakami:detail_info: ";

static constexpr std::string_view log_location_prefix_timing_event =
        "/:shirakami:timing:"; // +<event_name>

// receive string_view only
#define shirakami_binstring(arg)                                               \
    " " #arg "(len=" << (arg).size() << "):\"" << binary_printer((arg))        \
                     << "\"" //LINT

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
 * @brief logging level constant for stats information about gc
 */
static constexpr std::int32_t log_info_gc_stats = 33;

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

/**
 * @brief logging level constant for excessive traces
 */
static constexpr std::int32_t log_ex_trace = 100;

} // namespace shirakami
