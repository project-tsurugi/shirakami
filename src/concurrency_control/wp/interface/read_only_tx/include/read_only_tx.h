#pragma once

#include "concurrency_control/wp/include/session.h"

#include "shirakami/scheme.h"

namespace shirakami::read_only_tx {

extern Status abort(session* ti);

extern Status commit(session* ti);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         std::string& value, bool read_vlaue = true); // NOLINT

extern Status tx_begin(session* const ti);

} // namespace shirakami::read_only_tx