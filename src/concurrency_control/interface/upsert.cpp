

#include "atomic_wrapper.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/interface/include/helper.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "database/include/logging.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/binary_printer.h"
#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

void abort_update(session* ti) {
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        short_tx::abort(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::LONG) {
        long_tx::abort(ti);
    } else {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path.";
    }
}

static inline Status insert_process(session* const ti, Storage st,
                                    const std::string_view key,
                                    const std::string_view val) {
    Record* rec_ptr{};
    rec_ptr = new Record(key); // NOLINT
    yakushima::node_version64* nvp{};
    // create tombstone
    if (yakushima::status::OK ==
        put<Record>(ti->get_yakushima_token(), st, key, rec_ptr, nvp)) {
        if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
            // detail info
            if (logging::get_enable_logging_detail_info()) {
                VLOG(log_trace) << log_location_prefix_detail_info
                                << "insert record, key " + std::string(key);
            }

            Status check_node_set_res{ti->update_node_set(nvp)};
            if (check_node_set_res == Status::ERR_CC) {
                /**
                         * This This transaction is confirmed to be aborted 
                         * because the previous scan was destroyed by an insert
                         * by another transaction.
                         */
                abort_update(ti);
                ti->get_result_info().set_reason_code(
                        reason_code::CC_OCC_PHANTOM_AVOIDANCE);
                ti->get_result_info().set_key_storage_name(key, st);
                return Status::ERR_CC;
            }
        }
        ti->push_to_write_set({st, OP_TYPE::UPSERT, rec_ptr, val});
        return Status::OK;
    }
    // fail insert rec_ptr
    delete rec_ptr; // NOLINT
    return Status::WARN_CONCURRENT_INSERT;
}

Status upsert_body(Token token, Storage storage, const std::string_view key,
                   const std::string_view val) {
    // check constraint: key
    auto ret = check_constraint_key_length(key);
    if (ret != Status::OK) { return ret; }

    // take thread info
    auto* ti = static_cast<session*>(token);

    // check whether it already began.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    // check for write
    auto rc{check_before_write_ops(ti, storage, key, OP_TYPE::UPSERT)};
    if (rc != Status::OK) { return rc; }

    for (;;) {
        // index access to check local write set
        Record* rec_ptr{};
        if (Status::OK == get<Record>(storage, key, rec_ptr)) {
            // check local write
            write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
            if (in_ws != nullptr) {
                if (in_ws->get_op() == OP_TYPE::DELETE) {
                    in_ws->set_op(OP_TYPE::UPDATE);
                    in_ws->set_val(val);
                } else {
                    in_ws->set_val(val);
                }
                return Status::OK;
            }

            /**
             * If the target record has been deleted, change it to insert. 
             * The key needs to be present for later read own writes since the 
             * scan operation is performed on an existing key and may look up 
             * the local write set with that key.
             */
            tid_word dummy_tid{};
            rc = try_deleted_to_inserting(storage, key, rec_ptr, dummy_tid);
            if (rc == Status::WARN_NOT_FOUND) {
                // the rec_ptr is gced.
                goto INSERT_PROCESS; // NOLINT
            }
            if (rc == Status::OK) { // sharing tombstone
                // prepare insert / upsert with tombstone count
                ti->push_to_write_set({storage, OP_TYPE::UPSERT, rec_ptr, val,
                                       true}); // NOLINT
                return Status::OK;
            }
            if (rc == Status::WARN_ALREADY_EXISTS) {
                // prepare update
                ti->push_to_write_set(
                        {storage, OP_TYPE::UPSERT, rec_ptr, val}); // NOLINT
                return Status::OK;
            }
            if (rc == Status::WARN_CONCURRENT_INSERT) { continue; } // else
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path.";
        }

    INSERT_PROCESS:
        rc = insert_process(ti, storage, key, val);
        if (rc == Status::ERR_CC) { return rc; }
    }
}

Status upsert(Token token, Storage storage, std::string_view const key,
              std::string_view const val) {
    shirakami_log_entry << "upsert, token: " << token << ", storage; "
                        << storage << shirakami_binstring(key)
                        << shirakami_binstring(val);
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    auto ret = upsert_body(token, storage, key, val);
    ti->process_before_finish_step();
    shirakami_log_exit << "upsert, Status: " << ret;
    return ret;
}

} // namespace shirakami