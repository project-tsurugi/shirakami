

#include <string_view>

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status search_key(Token const token, Storage const storage,
                  std::string_view const key, Tuple*& tuple) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    if (ti->get_mode() == tx_mode::BATCH) {
        return batch::search_key(token, storage, key, tuple);
    } else if (ti->get_mode() == tx_mode::OCC) {
        return occ::search_key(token, storage, key, tuple);
    } else {
        LOG(FATAL) << "unreachable";
        std::abort();
    }
}

} // namespace shirakami
