
#include "concurrency_control/wp/include/session.h"

#include "shirakami/interface.h"

namespace shirakami {

Status abort([[maybe_unused]] Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);
    ti->clean_up_local_set();
    return Status::OK;
}

extern Status commit([[maybe_unused]] Token token, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    // occ
    // write lock
    // epoch load
    // serialization point
    // wp verify
    // read verify
    // node verify

    // batch

    // clean up local set
    auto* ti = static_cast<session*>(token);
    ti->clean_up_local_set();
    return Status::OK;
}

extern bool check_commit([[maybe_unused]] Token token, // NOLINT
                         [[maybe_unused]] std::uint64_t commit_id) {
    // todo
    return true;
}

} // namespace shirakami
