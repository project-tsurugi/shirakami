
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

namespace shirakami {

Status search_key(Token const token, Storage const storage, std::string_view const key,
                  Tuple** const tuple) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    if (ti->get_mode() == tx_mode::BATCH &&
        epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }

    // index access
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<const char*>(&storage), sizeof(storage)}, // NOLINT
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
        *ti->get_cache_for_search_ptr() = Tuple(in_ws->get_rec_ptr()->get_key(),in_ws->get_val());
        *tuple = ti->get_cache_for_search_ptr();

    }
    // version selection

    // occ
    // wp check
    // read version

    // batch
    // read version

    return Status::OK;
}

} // namespace shirakami
