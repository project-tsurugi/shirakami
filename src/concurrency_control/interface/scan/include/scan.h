
#pragma once

#include <string_view>

#include "shirakami/scheme.h"

namespace shirakami {

extern Status check_empty_scan_range(
        std::string_view l_key, scan_endpoint l_end,
        std::string_view r_key, scan_endpoint r_end);

} // namespace shirakami
