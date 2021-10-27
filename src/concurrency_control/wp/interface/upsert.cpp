

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status upsert(Token token, Storage storage, const std::string_view key,
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    if (ti->get_mode() == tx_mode::BATCH) {
        return batch::upsert(ti, storage, key, val);
    } else if (ti->get_mode() == tx_mode::OCC) {
        return occ::upsert(ti, storage, key, val);
    } else {
        LOG(FATAL) << "unreachable";
        std::abort();
    }
}

} // namespace shirakami
