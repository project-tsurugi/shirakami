

#include "atomic_wrapper.h"

#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status insert_process(session* const ti, Storage st, const std::string_view key,
                      const std::string_view val) {
    Record* rec_ptr = new Record(key, val); // NOLINT
    yakushima::node_version64* nvp{};
    if (yakushima::status::OK ==
        put<Record>(ti->get_yakushima_token(), st, key, rec_ptr, nvp)) {
        Status check_node_set_res{ti->update_node_set(nvp)};
        if (check_node_set_res == Status::ERR_PHANTOM) {
            /**
                         * This This transaction is confirmed to be aborted 
                         * because the previous scan was destroyed by an insert
                         * by another transaction.
                         */
            abort(ti);
            return Status::ERR_PHANTOM;
        }
        ti->get_write_set().push({st, OP_TYPE::INSERT, rec_ptr});
        return Status::OK;
    }
    // else insert_result == Status::WARN_ALREADY_EXISTS
    // so retry from index access
    delete rec_ptr; // NOLINT
    return Status::WARN_CONCURRENT_INSERT;
}

Status upsert(Token token, Storage storage, const std::string_view key,
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    // check
    if (ti->get_read_only()) {
        // can't write in read only mode.
        return Status::WARN_INVALID_HANDLE;
    }

    // update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    // batch mode check
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

    for (;;) {
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
        }

        auto rc{insert_process(ti, storage, key, val)};
        if (rc != Status::WARN_CONCURRENT_INSERT) { return rc; }
    }
}

} // namespace shirakami
