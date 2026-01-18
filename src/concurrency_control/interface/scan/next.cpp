
#include "atomic_wrapper.h"

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/interface/include/helper.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"
#include "database/include/logging.h"
#include "index/yakushima/include/interface.h"
#include "index/yakushima/include/scheme.h"


#include "shirakami/binary_printer.h"
#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

static Status next_body(Token const token, ScanHandle const handle) { // NOLINT
    auto* ti = static_cast<session*>(token);
    auto* sc = static_cast<scan_cache_obj*>(handle);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_handle().check_valid_scan_handle(sc) != Status::OK) {
        return Status::WARN_INVALID_HANDLE;
    }
    // valid handle

    // increment cursor
    for (;;) {
        auto& scan_index = sc->get_scan_index_ref();
        ++scan_index;

        auto& scan_buf = sc->get_vec();
        // check range of cursor
        if (scan_buf.size() <= scan_index) {
            scan_index = scan_buf.size(); // stop at scan_buf.size
            return Status::WARN_SCAN_LIMIT;
        }

        // check target record
        auto itr = scan_buf.begin() + scan_index; // NOLINT
        Record* rec_ptr = const_cast<Record*>(std::get<0>(*itr)); // NOLINT

        write_set_obj* inws{};
        if (ti->get_tx_type() !=
            transaction_options::transaction_type::READ_ONLY) {
            // check local write set
            inws = ti->get_write_set().search(rec_ptr);
            if (inws != nullptr) {
                /**
                 * If it exists and it is not delete operation, read from scan api
                 * call should be able to read the record.
                 */
                if (inws->get_op() == OP_TYPE::DELETE) { continue; }
                break;
            }
        }
        // not in local write set

        tid_word tid{loadAcquire(rec_ptr->get_tidw().get_obj())};
        if (!tid.get_absent()) {
            // normal page
            if (ti->get_tx_type() ==
                transaction_options::transaction_type::SHORT) {
                break;
            }
            if (ti->get_tx_type() ==
                        transaction_options::transaction_type::READ_ONLY) {
                if (tid.get_epoch() < ti->get_valid_epoch()) { break; }
                version* ver = rec_ptr->get_latest();
                for (;;) {
                    ver = ver->get_next();
                    if (ver == nullptr) { break; }
                    if (ver->get_tid().get_epoch() < ti->get_valid_epoch()) {
                        break;
                    }
                }
                if (ver != nullptr) {
                    // there is a readable rec
                    break;
                }
            } else {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "unreachable path";
                return Status::ERR_FATAL;
            }
        } else if (tid.get_latest()) {
            // inserting page
            // check read own inserting
            if (inws != nullptr) {
                if (inws->get_op() == OP_TYPE::INSERT) { break; }
            }

            // short tx should read inserting page
            if (ti->get_tx_type() ==
                transaction_options::transaction_type::SHORT) {
                break;
            }
            // rtx, ltx may read middle of version list
            if (tid.get_epoch() != 0) {
                // this is converting page, there may be readable rec
                if (tid.get_epoch() >= ti->get_valid_epoch()) {
                    // last version cant be read but it can read middle
                    version* ver = rec_ptr->get_latest();
                    for (;;) {
                        ver = ver->get_next();
                        if (ver == nullptr) { break; }
                        if (ver->get_tid().get_epoch() <
                            ti->get_valid_epoch()) {
                            break;
                        }
                    }
                    if (ver != nullptr) {
                        // there is a readable rec
                        break;
                    }
                } else {
                    // it can read latest version
                    break;
                }
            }
        } else {
            // absent && not latest == deleted
            if (ti->get_tx_type() ==
                transaction_options::transaction_type::SHORT) {
                /**
                 * short mode must read deleted record and verify, so add read set
                 */
                ti->push_to_read_set_for_stx({sc->get_storage(), rec_ptr, tid});
            }
            if (ti->get_tx_type() ==
                        transaction_options::transaction_type::READ_ONLY) {
                if (tid.get_epoch() >= ti->get_valid_epoch()) {
                    // there may be readable rec
                    version* ver = rec_ptr->get_latest();
                    for (;;) {
                        ver = ver->get_next();
                        if (ver == nullptr) { break; }
                        if (ver->get_tid().get_epoch() <
                            ti->get_valid_epoch()) {
                            break;
                        }
                    }
                    if (ver != nullptr) {
                        // there is a readable rec
                        break;
                    }
                }
            }
        }
    }

    // reset cache in cursor
    //ti->get_scan_handle().get_ci(handle).reset();
    return Status::OK;
}

Status next(Token const token, ScanHandle const handle) { // NOLINT
    shirakami_log_entry << "next, token: " << token << ", handle: " << handle;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term(), std::defer_lock};
        if (ti->get_mutex_flags().do_readaccess_daterm()) { lock.lock(); }
        ret = next_body(token, handle);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "next, Status: " << ret;
    return ret;
}

} // namespace shirakami
