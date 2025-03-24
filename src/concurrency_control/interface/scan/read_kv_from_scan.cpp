
#include "atomic_wrapper.h"

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp_meta.h"
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

Status read_from_scan(Token token, ScanHandle handle, bool key_read,
                      std::string& buf) {
    auto* ti = static_cast<session*>(token);

    // for register point read information.
    auto read_register_if_ltx = [ti](Record* rec_ptr) {
        if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
            ti->read_set_for_ltx().push(rec_ptr);
        }
    };
    auto register_read_version_max_epoch = [ti](epoch::epoch_t read_epoch) {
        if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
            if (read_epoch > ti->get_read_version_max_epoch()) {
                ti->set_read_version_max_epoch_if_need(read_epoch);
            }
        }
    };

    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    auto& sh = ti->get_scan_handle();

    Record* rec_ptr{};
    yakushima::node_version64* nv_ptr{};
    yakushima::node_version64_body nv{};
    {
        // take read lock
        std::shared_lock<std::shared_mutex> lk{sh.get_mtx_scan_cache()};
        // ==========
        /**
         * Check whether the handle is valid.
         */
        if (sh.get_scan_cache().find(handle) == sh.get_scan_cache().end()) {
            return Status::WARN_INVALID_HANDLE;
        }
        // ==========

        scan_handler::scan_elem_type target_elem;
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                sh.get_scan_cache()[handle]);
        std::size_t& scan_index = sh.get_scan_cache_itr()[handle];
        auto itr = scan_buf.begin() + scan_index; // NOLINT
        nv = std::get<1>(*itr);
        nv_ptr = std::get<2>(*itr);
        if (scan_buf.size() <= scan_index) { return Status::WARN_SCAN_LIMIT; }

        // ==========
        rec_ptr = const_cast<Record*>(std::get<0>(*itr)); // NOLINT
    }

    // log read range info if ltx
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        wp::wp_meta* wp_meta_ptr{};
        if (wp::find_wp_meta(sh.get_scanned_storage_set().get(handle),
                             wp_meta_ptr) != Status::OK) {
            // todo special case. interrupt DDL
            return Status::WARN_STORAGE_NOT_FOUND;
        }
        // update local read range
        std::string key_buf{};
        rec_ptr->get_key(key_buf);
        long_tx::update_local_read_range(ti, wp_meta_ptr, key_buf);
    }

    /**
     * Check read-own-write
     */
    if (ti->get_tx_type() != transaction_options::transaction_type::READ_ONLY) {
        const write_set_obj* inws = ti->get_write_set().search(rec_ptr);
        if (inws != nullptr) {
            if (inws->get_op() == OP_TYPE::DELETE) {
                read_register_if_ltx(rec_ptr);
                return Status::WARN_NOT_FOUND;
            }
            if (key_read) {
                inws->get_key(buf);
            } else {
                std::shared_lock<std::shared_mutex> lk{
                        rec_ptr->get_mtx_value()};
                inws->get_value(buf);
            }
            read_register_if_ltx(rec_ptr);
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
        ti->push_to_read_set_for_stx(
                {sh.get_scanned_storage_set().get(handle), rec_ptr, tidb});

        // create node set info, maybe phantom (Status::ERR_CC)
        auto rc = ti->get_node_set().emplace_back({nv, nv_ptr});
        if (rc == Status::ERR_CC) {
            std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
            ti->get_result_info().set_storage_name(
                    sh.get_scanned_storage_set().get(handle));
            ti->set_result(reason_code::CC_OCC_PHANTOM_AVOIDANCE);
            short_tx::abort(ti);
            return Status::ERR_CC;
        } // else: return result about read (rr: read result)
        return rr;
    }
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG ||
        ti->get_tx_type() == transaction_options::transaction_type::READ_ONLY) {
        version* ver{};
        bool is_latest{false};
        tid_word f_check{};
        auto rc{long_tx::version_function_with_optimistic_check(
                rec_ptr, ti->get_valid_epoch(), ver, is_latest, f_check)};
        if (rc == Status::WARN_NOT_FOUND) {
            // version list traversed.
            read_register_if_ltx(rec_ptr);
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
                read_register_if_ltx(rec_ptr);
                // check max epoch of read version
                register_read_version_max_epoch(f_check.get_epoch());
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
                read_register_if_ltx(rec_ptr);
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

        read_register_if_ltx(rec_ptr);
        // check max epoch of read version
        register_read_version_max_epoch(ver->get_tid().get_epoch());
        if (ver->get_tid().get_absent()) { return Status::WARN_NOT_FOUND; }
        // ok case
        if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
            std::string key_buf{};
            rec_ptr->get_key(key_buf);
            ti->insert_to_ltx_storage_read_set(
                    sh.get_scanned_storage_set().get(handle), key_buf);
        }
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
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term()};
        ret = read_from_scan(token, handle, true, key);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "read_key_from_scan, Status: " << ret
                       << ", key: " << key;
    return ret;
}

Status read_value_from_scan(Token const token, // NOLINT
                            ScanHandle const handle, std::string& value) {
    shirakami_log_entry << "read_value_from_scan, token: " << token
                        << ", handle: " << handle;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term()};
        ret = read_from_scan(token, handle, false, value);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "read_value_from_scan, Status: " << ret << ", value: "
                       << shirakami_binstring(std::string_view(value));
    return ret;
}

} // namespace shirakami
