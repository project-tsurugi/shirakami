
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/include/helper.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"
#include "concurrency_control/wp/interface/short_tx/include/short_tx.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"
#include "shirakami/logging.h"

namespace shirakami {

Status update(Token token, Storage storage,
              const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    ti->process_before_start_step();

    // check for write
    auto rc{check_before_write_ops(ti, storage, OP_TYPE::UPDATE)};
    if (rc != Status::OK) {
        ti->process_before_finish_step();
        return rc;
    }

    // index access to check local write set
    Record* rec_ptr{};
    if (Status::OK == get<Record>(storage, key, rec_ptr)) {
        // check local write
        write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
        if (in_ws != nullptr) {
            if (in_ws->get_op() == OP_TYPE::DELETE) {
                ti->process_before_finish_step();
                return Status::WARN_ALREADY_DELETE;
            }
            if (in_ws->get_op() == OP_TYPE::INSERT) {
                in_ws->get_rec_ptr()->get_latest()->set_value(val);
            } else {
                in_ws->set_op(OP_TYPE::UPSERT);
                in_ws->set_val(val);
            }
            ti->process_before_finish_step();
            return Status::WARN_WRITE_TO_LOCAL_WRITE;
        }

        // check absent
        tid_word ctid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        if (ctid.get_absent()) {
            ti->process_before_finish_step();
            return Status::WARN_NOT_FOUND;
        }

        // prepare write
        ti->get_write_set().push(
                {storage, OP_TYPE::UPDATE, rec_ptr, val}); // NOLINT
        ti->process_before_finish_step();
        return Status::OK;
    }
    ti->process_before_finish_step();
    return Status::WARN_NOT_FOUND;
}

} // namespace shirakami
