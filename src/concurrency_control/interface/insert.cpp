
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

static void abort_insert(session* const ti) {
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        short_tx::abort(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::LONG) {
        long_tx::abort(ti);
    } else {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix << "library programming error";
    }
}
static inline Status insert_process(session* const ti, Storage st,
                                    const std::string_view key,
                                    const std::string_view val,
                                    Record*& out_rec_ptr,
                                    std::vector<blob_id_type>& lobs) {
    Record* rec_ptr{};
    rec_ptr = new Record(key); // NOLINT
    tid_word tid{rec_ptr->get_tidw()};
    rec_ptr->get_shared_tombstone_count().store(1, std::memory_order_release);

    yakushima::node_version64* nvp{};
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
                abort_insert(ti);
                std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
                ti->get_result_info().set_reason_code(
                        reason_code::CC_OCC_PHANTOM_AVOIDANCE);
                ti->get_result_info().set_key_storage_name(key, st);
                return Status::ERR_CC;
            }
            if (check_node_set_res == Status::OK) {
                // newly created Record on previously read node; add to read set
                ti->push_to_read_set_for_stx({st, rec_ptr, tid});
            }
        }
        ti->push_to_write_set({st, OP_TYPE::INSERT, rec_ptr, val, true, std::move(lobs)});
        out_rec_ptr = rec_ptr;
        return Status::OK;
    }
    // else insert_result == Status::WARN_ALREADY_EXISTS
    // so retry from index access
    delete rec_ptr; // NOLINT
    return Status::WARN_CONCURRENT_INSERT;
}

static void register_read_if_ltx(session* const ti, Record* const rec_ptr) {
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        ti->read_set_for_ltx().push(rec_ptr);
    }
}

static Status insert_body(
        Token const token, Storage const storage, // NOLINT
        const std::string_view key, const std::string_view val,
        blob_id_type const* blobs_data, std::size_t blobs_size) {
    // check constraint: key
    auto ret = check_constraint_key_length(key);
    if (ret != Status::OK) { return ret; }

    // take thread info
    auto* ti = static_cast<session*>(token);

    // check it already began.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    // check for write
    auto rc{check_before_write_ops(ti, storage, key, OP_TYPE::INSERT)};
    if (rc != Status::OK) { return rc; }

    std::vector<blob_id_type> lobs(blobs_size);
    if (blobs_size != 0) {
        lobs.assign(blobs_data, blobs_data + blobs_size); // NOLINT
    }
    for (;;) {
        // index access to check local write set
        Record* rec_ptr{};
        if (Status::OK == get<Record>(storage, key, rec_ptr)) {
            write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)};
            if (in_ws != nullptr) {
                if (in_ws->get_op().is_wso_to_alive()) {
                    return Status::WARN_ALREADY_EXISTS;
                }
                in_ws->set_op(in_ws->get_op().of_wso_to_alive());
                in_ws->set_val(val);
                in_ws->set_lobs(std::move(lobs));
                return Status::OK;
            }

            tid_word found_tid{};
            rc = try_deleted_to_inserting(storage, key, rec_ptr, found_tid);
            if (rc == Status::OK) { // ok already count up tombstone count
                ti->push_to_write_set({storage, OP_TYPE::INSERT, rec_ptr, val, true, std::move(lobs)});
                register_read_if_ltx(ti, rec_ptr);
                return Status::OK;
            }
            if (rc == Status::WARN_CONCURRENT_INSERT) { continue; }
            if (rc == Status::WARN_NOT_FOUND) {
                // the rec_ptr is gced;
                goto INSERT_PROCESS; // NOLINT
            }
            if (rc == Status::WARN_ALREADY_EXISTS) {
                // ==========
                // start: make read set
                if (ti->get_tx_type() ==
                    transaction_options::transaction_type::SHORT) {
                    ti->push_to_read_set_for_stx({storage, rec_ptr, found_tid});
                } else if (ti->get_tx_type() ==
                           transaction_options::transaction_type::LONG) {
                    // register read_by_set
                    register_read_if_ltx(ti, rec_ptr);
                } else {
                    LOG_FIRST_N(ERROR, 1) << log_location_prefix
                                          << "library programming error";
                    return Status::ERR_FATAL;
                }
                // end: make read set
                // ==========
            }
            return rc;
        }

    INSERT_PROCESS: // NOLINT
        auto rc{insert_process(ti, storage, key, val, rec_ptr, lobs)};
        if (rc == Status::OK) {
            register_read_if_ltx(ti, rec_ptr);
            return Status::OK;
        }
        if (rc == Status::ERR_CC) { return rc; }
    }
}

Status insert(Token const token, Storage const storage, // NOLINT
              const std::string_view key,
              const std::string_view val,
              blob_id_type const* blobs_data,
              std::size_t blobs_size) {
    shirakami_log_entry << "insert, token: " << token << ", storage: " << storage
                        << "," shirakami_binstring(key) "," shirakami_binstring(val)
                           ", blobs_data: " << blobs_data << ", blobs_size: " << blobs_size
                        << " " << span_printer(blobs_data, blobs_size);
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term()};

        // insert_body check warn not begin
        ret = insert_body(token, storage, key, val, blobs_data, blobs_size);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "insert, Status: " << ret;
    return ret;
}

} // namespace shirakami
