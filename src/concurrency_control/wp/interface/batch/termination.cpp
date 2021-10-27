
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "concurrency_control/wp/interface/batch/include/batch.h"

namespace shirakami::batch {

Status abort(Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);
    ti->clean_up();
    return Status::OK;
}

extern Status commit(Token token, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    auto* ti = static_cast<session*>(token);

    // clean up
    ti->clean_up();
    return Status::OK;
}

} // namespace shirakami::batch