#pragma once

#include <cstdint>

namespace shirakami {

static constexpr std::string_view log_location_prefix = "/:shirakami ";

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
 * @brief logging level constant for debug information
 */
static constexpr std::int32_t log_debug = 40;

/**
 * @brief logging level constant for traces
 */
static constexpr std::int32_t log_trace = 50;

} // namespace shirakami