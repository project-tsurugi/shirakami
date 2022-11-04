#pragma once

#include <string_view>
#include <vector>

#include "concurrency_control/include/session.h"

#include "shirakami/scheme.h"
#include "shirakami/tuple.h"

namespace shirakami::short_tx {

extern Status abort(session* ti);

extern Status commit(session* ti);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         std::string& value, bool read_value = true); // NOLINT

} // namespace shirakami::short_tx