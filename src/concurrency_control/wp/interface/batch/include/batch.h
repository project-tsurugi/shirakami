#pragma once

#include <string_view>
#include <vector>

#include "concurrency_control/wp/include/session.h"

#include "shirakami/scheme.h"
#include "shirakami/tuple.h"

namespace shirakami::batch {

extern Status abort(session* ti);

extern Status commit(session* ti, commit_param* cp);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         Tuple*& tuple, bool read_value = true); // NOLINT

extern Status tx_begin(session* ti, std::vector<Storage> write_preserve);

extern Status upsert(session* ti, Storage storage, std::string_view key,
                     std::string_view val);

} // namespace shirakami::batch