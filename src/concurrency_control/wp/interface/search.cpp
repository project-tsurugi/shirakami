

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

Status exist_key(Storage storage, std::string_view const key) {
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            key))};                                               // NOLINT
    if (rec_d_ptr == nullptr) { return Status::WARN_NOT_FOUND; }

    return Status::OK;
}

Status search_key(Token const token, Storage const storage,
                  std::string_view const key, Tuple*& tuple) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    // update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    if (ti->get_mode() == tx_mode::BATCH) {
        return batch::search_key(ti, storage, key, tuple);
    }
    if (ti->get_mode() == tx_mode::OCC) {
        return occ::search_key(ti, storage, key, tuple);
    }
    LOG(FATAL) << "unreachable";
    return Status::ERR_FATAL;
}

} // namespace shirakami
