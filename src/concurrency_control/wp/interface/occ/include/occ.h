#pragma once

#include <string_view>

#include "concurrency_control/wp/include/session.h"

#include "shirakami/scheme.h"
#include "shirakami/tuple.h"

namespace shirakami::occ {

extern Status abort(session* ti);

extern Status commit(session* ti, commit_param* cp);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         Tuple*& tuple);

extern Status upsert(session* ti, Storage storage, std::string_view key,
                     std::string_view val);

} // namespace shirakami::occ