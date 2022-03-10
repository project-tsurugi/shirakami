
#include "atomic_wrapper.h"

#include "concurrency_control/include/tuple_local.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/include/helper.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

inline Status insert_process(session* const ti, Storage st,
                             const std::string_view key,
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

Status try_deleted_to_inserted(Record* const rec_ptr,
                               std::string_view const val) {
    tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
    // point 1
    if (check.get_latest() && check.get_absent()) {
        return Status::WARN_CONCURRENT_INSERT;
    }
    if (!check.get_absent()) { return Status::WARN_ALREADY_EXISTS; }
    // The page was deleted at point 1.

    rec_ptr->get_tidw_ref().lock();
    // point 2
    tid_word tid{rec_ptr->get_tidw_ref()};
    if (tid.get_absent()) {
        // success
        tid.set_latest(true);
        rec_ptr->set_tid(tid);
        rec_ptr->set_value(val);
        return Status::OK;
    } else {
        /**
         * The deleted page was changed to living page by someone between 
         * point 1 and point 2.
         */
        rec_ptr->get_tidw_ref().unlock();
        return Status::WARN_ALREADY_EXISTS;
    }
}

Status insert(Token const token, Storage const storage,
              const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    // check for write
    auto rc{check_before_write_ops(ti, storage, OP_TYPE::INSERT)};
    if (rc != Status::OK) { return rc; }

    // update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    for (;;) {
        // index access to check local write set
        Record* rec_ptr{};
        if (Status::OK == get<Record>(storage, key, rec_ptr)) {
            write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)};
            if (in_ws != nullptr) {
                if (in_ws->get_op() == OP_TYPE::INSERT) {
                    return Status::WARN_ALREADY_EXISTS;
                }
                if (in_ws->get_op() == OP_TYPE::DELETE) {
                    in_ws->set_op(OP_TYPE::UPDATE);
                    in_ws->set_val(val);
                    LOG(INFO);
                    return Status::OK;
                }
            }

            rc = try_deleted_to_inserted(rec_ptr, val);
            if (rc == Status::OK) {
                ti->get_write_set().push({storage, OP_TYPE::INSERT, rec_ptr});
                return Status::OK;
            } else {
                return rc;
            }
        }

        auto rc{insert_process(ti, storage, key, val)};
        if (rc == Status::OK) { return Status::OK; }
    }
}

} // namespace shirakami
