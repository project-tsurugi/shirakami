//
// Created by thawk on 2020/09/14.
//

/**
 * @file src/index/yakushima/include/scheme.h
 */

#pragma once

#include "shirakami/scheme.h"

#include "yakushima/include/scheme.h"

namespace shirakami {

static inline yakushima::scan_endpoint
parse_scan_endpoint(shirakami::scan_endpoint s_end) {
    switch (s_end) {
        case (shirakami::scan_endpoint::EXCLUSIVE):
            return yakushima::scan_endpoint::EXCLUSIVE;
        case (shirakami::scan_endpoint::INCLUSIVE):
            return yakushima::scan_endpoint::INCLUSIVE;
        case (shirakami::scan_endpoint::INF):
            return yakushima::scan_endpoint::INF;
        default:
            std::cout << __FILE__ << " : " << __LINE__ << " : error"
                      << std::endl; // NOLINT(*-avoid-endl)
            std::abort();
    }
}

} // namespace shirakami
