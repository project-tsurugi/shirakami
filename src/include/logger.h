//
// Created by thawk on 2020/11/10.
//

#pragma once

#ifdef NDEBUG
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
#else
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG // NOLINT
#define SPDLOG_DEBUG_ON
#endif

#include <spdlog/spdlog.h>

namespace shirakami::logger {

static inline auto loggersink = std::make_shared<spdlog::sinks::stdout_sink_mt>();                                  // NOLINT
static inline auto shirakami_logger = std::make_shared<spdlog::async_logger>("shirakami_logger", loggersink, 8192); // NOLINT

static inline void setup_spdlog() {

#ifdef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#else
    spdlog::set_level(spdlog::level::debug);
#endif
}

} // namespace shirakami::logger