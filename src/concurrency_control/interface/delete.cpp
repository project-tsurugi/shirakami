
#include "atomic_wrapper.h"

#include "storage.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/local_set.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/include/helper.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "database/include/logging.h"
#include "index/yakushima/include/interface.h"

#include "shirakami/binary_printer.h"
#include "shirakami/interface.h"
#include "shirakami/logging.h"

namespace shirakami {

static void register_read_if_ltx(session* const ti, Record* const rec_ptr) {
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        ti->read_set_for_ltx().push(rec_ptr);
    }
}

static void process_before_return_not_found(session* const ti,
                                            Storage const storage,
                                            std::string_view const key) {
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        /**
         * Normally, read information is stored at page, but the page is not
         * found. So it stores at table level information as range,
         * key <= range <= key.
         */
        // get page set meta info
        wp::page_set_meta* psm{};
        auto rc = wp::find_page_set_meta(storage, psm);
        if (rc != Status::OK) {
            LOG_FIRST_N(ERROR, 1)
                    << log_location_prefix
                    << "unexpected error. library programming error or "
                       "usage error (mixed ddl and dml?)";
            return;
        }
        // get range read  by info
        range_read_by_long* rrbp{psm->get_range_read_by_long_ptr()};
        ti->get_range_read_set_for_ltx().insert(std::make_tuple(
                rrbp, std::string(key), scan_endpoint::INCLUSIVE,
                std::string(key), scan_endpoint::INCLUSIVE));
    }
}

static Status delete_record_body(Token token, Storage storage,
                          const std::string_view key) { // NOLINT
    // check constraint: key
    auto ret = check_constraint_key_length(key);
    if (ret != Status::OK) { return ret; }

    // process about worker
    auto* ti = static_cast<session*>(token);

    // check whether it already began.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    // check for write
    auto rc{check_before_write_ops(ti, storage, key, OP_TYPE::DELETE)};
    if (rc != Status::OK) { return rc; }

    // index access to check local write set
    Record* rec_ptr{};
    rc = get<Record>(storage, key, rec_ptr);
    if (Status::OK == rc) {
        // check local write
        write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
        if (in_ws != nullptr) {
            if (in_ws->get_op().is_wso_to_absent()) {
                return Status::WARN_NOT_FOUND;
            }
            in_ws->set_op(in_ws->get_op().of_wso_to_absent());
            return Status::OK;
        }
        // check absent
        tid_word ctid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        if (ctid.get_absent()) {
            process_before_return_not_found(ti, storage, key);
            return Status::WARN_NOT_FOUND;
        }
        // prepare write
        ti->push_to_write_set({storage, OP_TYPE::DELETE, rec_ptr, false});
        register_read_if_ltx(ti, rec_ptr);
        return Status::OK;
    }
    if (rc == Status::WARN_NOT_FOUND) {
        process_before_return_not_found(ti, storage, key);
        return Status::WARN_NOT_FOUND;
    }
    if (rc == Status::WARN_STORAGE_NOT_FOUND) {
        return Status::WARN_STORAGE_NOT_FOUND;
    }
    LOG_FIRST_N(ERROR, 1) << log_location_prefix
                          << "library programming error: " << rc;
    return Status::ERR_FATAL;
}

Status delete_record(Token token, Storage storage, const std::string_view key) {
    shirakami_log_entry << "delete_record, token: " << token
                        << ", storage: " << storage << "," shirakami_binstring(key);
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term()};

        // delete record body check warn not begin for strand
        ret = delete_record_body(token, storage, key);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "delete_record, Status: " << ret;
    return ret;
}

} // namespace shirakami
