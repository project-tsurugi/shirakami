#pragma once

#include <string_view>
#include <vector>

#include "concurrency_control/wp/include/session.h"

#include "shirakami/scheme.h"
#include "shirakami/tuple.h"

namespace shirakami::long_tx {

extern Status abort(session* ti);

extern Status commit(session* ti, commit_param* cp);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         std::string& value, bool read_value = true); // NOLINT

extern Status tx_begin(session* ti, std::vector<Storage> write_preserve);

} // namespace shirakami::long_tx