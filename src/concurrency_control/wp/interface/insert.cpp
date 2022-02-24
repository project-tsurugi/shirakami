
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

Status try_deleted_to_inserted([[maybe_unused]] session* ti,
                               [[maybe_unused]] Storage st,
                               [[maybe_unused]] std::string_view key,
                               [[maybe_unused]] std::string_view val,
                               [[maybe_unused]] Record* rec_ptr) {
    rec_ptr->get_tidw_ref().lock();
    tid_word tid{rec_ptr->get_tidw_ref()};
    if (tid.get_absent()) {
        tid.set_latest(true);
        rec_ptr->set_tid(tid);
        return Status::OK;
    } else {
        rec_ptr->get_tidw_ref().unlock();
        return Status::WARN_ALREADY_EXISTS;
    }
}

Status insert(Token token, Storage storage,
              const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    // check for write
    auto rc{check_before_write_ops(ti, storage)};
    if (rc != Status::OK) { return rc; }

    // update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    for (;;) {
        // index access to check local write set
        Record* rec_ptr{};
        if (Status::OK == get<Record>(storage, key, rec_ptr)) {
            rc = try_deleted_to_inserted(ti, storage, key, val, rec_ptr);
            if (rc == Status::OK) {
                ti->get_write_set().push({storage, OP_TYPE::INSERT, rec_ptr});
                return Status::OK;
            }
            if (rc == Status::WARN_ALREADY_EXISTS) {
                return Status::WARN_ALREADY_EXISTS;
            }
            LOG(ERROR) << "error. impossible code path.";
            return Status::ERR_FATAL;
        }

        auto rc{insert_process(ti, storage, key, val)};
        if (rc == Status::OK) { return Status::OK; }
    }
}

} // namespace shirakami
