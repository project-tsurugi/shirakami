
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "concurrency_control/wp/interface/batch/include/batch.h"

namespace shirakami::batch {

Status abort(session* ti) { // NOLINT
    // clean up wp

    // clean up local set
    ti->clean_up();
    return Status::OK;
}

extern Status commit(session* ti, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    // clean up
    ti->clean_up();
    return Status::OK;
}

} // namespace shirakami::batch