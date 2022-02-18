
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "shirakami/interface.h"

namespace shirakami {

Status update(Token token, Storage storage,
              const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    if (ti->get_read_only()) {
        // can't write in read only mode.
        return Status::WARN_INVALID_HANDLE;
    }

    //update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    // batch check
    if (ti->get_mode() == tx_mode::BATCH) {
        if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
            // not in valid epoch.
            return Status::WARN_PREMATURE;
        }
        if (!ti->check_exist_wp_set(storage)) {
            // can't write without wp.
            return Status::WARN_INVALID_ARGS;
        }
    }

    // index access to check local write set
    Record* rec_ptr{};
    if (Status::OK == get<Record>(storage, key, rec_ptr)) {
        // check local write
        write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
        if (in_ws != nullptr) {
            if (in_ws->get_op() == OP_TYPE::INSERT) {
                in_ws->get_rec_ptr()->get_latest()->set_value(val);
            } else {
                in_ws->set_op(OP_TYPE::UPSERT);
                in_ws->set_val(val);
            }
            return Status::WARN_WRITE_TO_LOCAL_WRITE;
        }

        // prepare write
        ti->get_write_set().push(
                {storage, OP_TYPE::UPSERT, rec_ptr, val}); // NOLINT
        return Status::OK;
    } else {
        return Status::WARN_NOT_FOUND;
    }
}

} // namespace shirakami
