#pragma once

#include "concurrency_control/include/session.h"

#include "shirakami/scheme.h"

namespace shirakami::read_only_tx {

extern Status abort(session* ti);

extern Status commit(session* ti);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         std::string& value, bool read_value = true); // NOLINT

extern Status tx_begin(session* ti);

} // namespace shirakami::read_only_tx
