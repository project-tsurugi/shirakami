
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "concurrency_control/wp/interface/batch/include/batch.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status abort(Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);

    if (ti->get_mode() == tx_mode::BATCH) { return batch::abort(ti); }
    return occ::abort(ti);
}

Status commit([[maybe_unused]] Token token, // NOLINT
              [[maybe_unused]] commit_param* cp) {
    auto* ti = static_cast<session*>(token);

    if (ti->get_mode() == tx_mode::BATCH) { return batch::commit(ti, cp); }
    return occ::commit(ti, cp);
}

bool check_commit([[maybe_unused]] Token token, // NOLINT
                  [[maybe_unused]] std::uint64_t commit_id) {
    // todo
    return true;
}

} // namespace shirakami
