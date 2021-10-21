

#include <string_view>

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status search_key(Token const token, Storage const storage,
                  std::string_view const key, Tuple** const tuple) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    if (ti->get_mode() == tx_mode::BATCH &&
        epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }

    // index access
    Record** rec_d_ptr{std::get<0>(
            yakushima::get<Record*>({reinterpret_cast<const char*>(&storage),
                                     sizeof(storage)}, // NOLINT
                                    key))};
    if (rec_d_ptr == nullptr) {
        *tuple = nullptr;
        return Status::WARN_NOT_FOUND;
    }
    Record* rec_ptr{*rec_d_ptr};

    // check local write set
    write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (in_ws != nullptr) {
        if (in_ws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        ti->get_cache_for_search_ptr()->get_pimpl()->set_key(
                in_ws->get_rec_ptr()->get_key());
        ti->get_cache_for_search_ptr()->get_pimpl()->set_val(in_ws->get_val());
        *tuple = ti->get_cache_for_search_ptr();
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    return Status::OK;
#if 0
    if (ti->get_mode() == tx_mode::OCC) {
        // occ
        // wp check

    } else {
        // batch
        return Status::OK;
    }
#endif
    // version selection

    // read version

    // read version
}

} // namespace shirakami
