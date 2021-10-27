#pragma once

#include "shirakami/scheme.h"
#include "shirakami/tuple.h"

namespace shirakami::batch {

extern Status abort(Token token);

extern Status commit(Token token, commit_param* cp);

extern Status search_key(Token token, Storage storage, std::string_view key,
                         Tuple*& tuple);

extern Status upsert(Token token, Storage storage, std::string_view key,
                     std::string_view val);

} // namespace shirakami::batch