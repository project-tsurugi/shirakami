#pragma once

#include "concurrency_control/wp/include/session.h"

#include "shirakami/scheme.h"

namespace shirakami::read_only_tx {

extern Status abort(session* ti);

extern Status tx_begin(session* const ti);

extern Status commit(session* ti);

} // namespace shirakami::read_only_tx