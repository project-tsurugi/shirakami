
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

static Status read_from_scan(Token token, ScanHandle handle, bool key_read,
                             std::string& buf) {
    auto* ti = static_cast<session*>(token);

    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    auto* sc = static_cast<scan_cache_obj*>(handle);
    auto& sh = ti->get_scan_handle();

    /**
     * Check whether the handle is valid.
     */
    if (sh.check_valid_scan_handle(sc) != Status::OK) {
        return Status::WARN_INVALID_HANDLE;
    }

    auto& scan_buf = sc->get_vec();
    auto scan_index = sc->get_scan_index();
    if (scan_buf.size() <= scan_index) { return Status::WARN_SCAN_LIMIT; }
    auto itr = scan_buf.begin() + scan_index; // NOLINT
    yakushima::node_version64_body nv = std::get<1>(*itr);
    yakushima::node_version64* nv_ptr = std::get<2>(*itr);

    Record* rec_ptr = const_cast<Record*>(std::get<0>(*itr)); // NOLINT

    /**
     * Check read-own-write
     */
    if (ti->get_tx_type() != transaction_options::transaction_type::READ_ONLY) {
        const write_set_obj* inws = ti->get_write_set().search(rec_ptr);
        if (inws != nullptr) {
            if (inws->get_op() == OP_TYPE::DELETE) {
                return Status::WARN_NOT_FOUND;
            }
            if (key_read) {
                inws->get_key(buf);
            } else {
                std::shared_lock<std::shared_mutex> lk{
                        rec_ptr->get_mtx_value()};
                inws->get_value(buf);
            }
            return Status::OK;
        }
    }
    // ==========

    // check target record
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        tid_word tidb{};
        std::string valueb{};
        Status rr{};
        if (key_read) {
            rec_ptr->get_key(buf);
            rr = read_record(rec_ptr, tidb, valueb, false);
        } else {
            rr = read_record(rec_ptr, tidb, buf);
        }
        if (rr == Status::WARN_CONCURRENT_UPDATE) { return rr; }
        // note: update can't log effect, but insert / delete log read effect.
        // optimization: set for re-read
        ti->push_to_read_set_for_stx({sc->get_storage(), rec_ptr, tidb});

        // create node set info, maybe phantom (Status::ERR_CC)
        auto rc = ti->get_node_set().emplace_back({nv, nv_ptr});
        if (rc == Status::ERR_CC) {
            std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
            ti->get_result_info().set_storage_name(sc->get_storage());
            ti->set_result(reason_code::CC_OCC_PHANTOM_AVOIDANCE);
            short_tx::abort(ti);
            return Status::ERR_CC;
        } // else: return result about read (rr: read result)
        return rr;
    }
    if (ti->get_tx_type() == transaction_options::transaction_type::READ_ONLY) {
        version* ver{};
        bool is_latest{false};
        tid_word f_check{};
        auto rc{long_tx::version_function_with_optimistic_check(
                rec_ptr, ti->get_valid_epoch(), ver, is_latest, f_check)};
        if (rc == Status::WARN_NOT_FOUND) {
            // version list traversed.
            return rc;
        }
        if (rc != Status::OK) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return Status::ERR_FATAL;
        }

        // read latest version after version function
        if (is_latest) {
            if (!f_check.get_absent()) {
                if (key_read) {
                    rec_ptr->get_key(buf);
                } else {
                    ver->get_value(buf);
                }
            }
            // load stable timestamp to verify optimistic read
            tid_word s_check{loadAcquire(&rec_ptr->get_tidw_ref().get_obj())};
            for (;;) {
                if (s_check.get_lock()) {
                    _mm_pause();
                    s_check = loadAcquire(&rec_ptr->get_tidw_ref().get_obj());
                    continue;
                }
                break;
            }
            // verify optimistic read
            if (s_check.get_obj() == f_check.get_obj()) {
                // success optimistic read latest version
                // check max epoch of read version
                if (f_check.get_absent()) { return Status::WARN_NOT_FOUND; }
                return Status::OK;
            }
            /**
             * else: fail to do optimistic read latest version. retry version
             * function.
             * It may find null version.
             */
            ver = rec_ptr->get_latest();
            rc = long_tx::version_function_without_optimistic_check(
                    ti->get_valid_epoch(), ver);
            if (rc == Status::WARN_NOT_FOUND) {
                // version list traversed.
                return rc;
            }
        }

        if (!ver->get_tid().get_absent()) {
            // read non-latest version after version function
            if (key_read) {
                rec_ptr->get_key(buf);
            } else {
                ver->get_value(buf);
            }
        }

        // check max epoch of read version
        if (ver->get_tid().get_absent()) { return Status::WARN_NOT_FOUND; }
        // ok case
        return Status::OK;
    }

    LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
    return Status::ERR_FATAL;
}

Status read_key_from_scan(Token const token, ScanHandle const handle, // NOLINT
                          std::string& key) {
    shirakami_log_entry << "read_key_from_scan, token: " << token
                        << ", handle: " << handle;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term(), std::defer_lock};
        if (ti->get_mutex_flags().do_readaccess_daterm()) { lock.lock(); }
        ret = read_from_scan(token, handle, true, key);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "read_key_from_scan, Status: " << ret << "," shirakami_binstring(key);
    return ret;
}

Status read_value_from_scan(Token const token, ScanHandle const handle, // NOLINT
                            std::string& value) {
    shirakami_log_entry << "read_value_from_scan, token: " << token
                        << ", handle: " << handle;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term(), std::defer_lock};
        if (ti->get_mutex_flags().do_readaccess_daterm()) { lock.lock(); }
        ret = read_from_scan(token, handle, false, value);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "read_value_from_scan, Status: " << ret << "," shirakami_binstring(value);
    return ret;
}

} // namespace shirakami
