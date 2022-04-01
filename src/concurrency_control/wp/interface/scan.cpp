
#include "concurrency_control/include/tuple_local.h"

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

#include "index/yakushima/include/interface.h"
#include "index/yakushima/include/scheme.h"


#include "shirakami/interface.h"

namespace shirakami {

Status close_scan(Token const token, ScanHandle const handle) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    ti->process_before_start_step();

    ti->process_before_finish_step();
    return ti->get_scan_handle().clear(handle);
}

inline Status find_open_scan_slot(session* const ti, ScanHandle& out) {
    auto& sh = ti->get_scan_handle();
    for (ScanHandle i = 0;; ++i) {
        auto itr = sh.get_scan_cache().find(i);
        if (itr == sh.get_scan_cache().end()) {
            out = i;
            // clear cursor info
            sh.get_scan_cache_itr()[i] = 0;
            return Status::OK;
        }
    }
    return Status::WARN_SCAN_LIMIT;
}

Status open_scan(Token const token, Storage storage,
                 const std::string_view l_key, const scan_endpoint l_end,
                 const std::string_view r_key, const scan_endpoint r_end,
                 ScanHandle& handle, std::size_t const max_size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    ti->process_before_start_step();

    if (ti->get_tx_type() == TX_TYPE::LONG) {
        if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
            return Status::WARN_PREMATURE;
        }
    }

    if (ti->get_read_only()) {
        // todo stale snapshot read only tx mode.
        ti->process_before_finish_step();
        return Status::ERR_NOT_IMPLEMENTED;
    }

    auto rc = find_open_scan_slot(ti, handle);
    if (rc != Status::OK) {
        ti->process_before_finish_step();
        return rc;
    }

    // scan for index
    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    constexpr std::size_t index_rec_ptr{1};
    std::vector<std::pair<yakushima::node_version64_body,
                          yakushima::node_version64*>>
            nvec;
    constexpr std::size_t index_nvec_body{0};
    constexpr std::size_t index_nvec_ptr{1};
    rc = scan(storage, l_key, l_end, r_key, r_end, max_size, scan_res, &nvec);
    if (rc != Status::OK) {
        ti->process_before_finish_step();
        return rc;
    }

    range_read_by_bt* rbp{};
    rc = wp::find_read_by(storage, rbp);
    if (rc == Status::OK) {
        /**
          * register read_by_set
          * todo: enhancement: 
          * The range is modified according to the execution of 
          * read_from_scan, and the range is fixed and registered at the end of 
          * the transaction.
          */
        ti->get_range_read_by_bt_set().emplace_back(
                std::make_tuple(rbp, l_key, l_end, r_key, r_end));
    } else {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    /**
     * You must ensure that new elements are not interrupted in the range at 
     * the node that did not retrieve the element but scanned it when masstree 
     * scanned it.
     */
    std::size_t nvec_delta{0};
    if (scan_res.size() < nvec.size()) {
        auto add_ns = [&ti, &nvec](std::size_t n) {
            for (std::size_t i = 0; i < n; ++i) {
                ti->get_node_set().emplace_back(nvec.at(i));
            }
        };
        if (scan_res.size() + 1 == nvec.size()) {
            nvec_delta = 1;
            add_ns(1);

        } else if (scan_res.size() + 2 == nvec.size()) {
            nvec_delta = 2;
            add_ns(2);
        }
    }

    // Cache a pointer to record.
    auto& sh = ti->get_scan_handle();
    std::get<scan_handler::scan_cache_storage_pos>(
            sh.get_scan_cache()[handle]) = storage;
    auto& vec = std::get<scan_handler::scan_cache_vec_pos>(
            sh.get_scan_cache()[handle]);
    vec.reserve(scan_res.size());
    for (std::size_t i = 0; i < scan_res.size(); ++i) {
        vec.emplace_back(*std::get<index_rec_ptr>(scan_res.at(i)),
                         std::get<index_nvec_body>(nvec.at(i + nvec_delta)),
                         std::get<index_nvec_ptr>(nvec.at(i + nvec_delta)));
    }

    sh.get_scanned_storage_set().set(handle, storage);
    ti->process_before_finish_step();
    return Status::OK;
}

Status next(Token const token, ScanHandle const handle) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    ti->process_before_start_step();

    auto& sh = ti->get_scan_handle();
    /**
     * Check whether the handle is valid.
     */
    if (sh.get_scan_cache().find(handle) == sh.get_scan_cache().end()) {
        ti->process_before_finish_step();
        return Status::WARN_INVALID_HANDLE;
    }

    // increment cursor
    std::size_t& scan_index = sh.get_scan_cache_itr()[handle];
    ++scan_index;

    // check range of cursor
    if (std::get<scan_handler::scan_cache_vec_pos>(sh.get_scan_cache()[handle])
                .size() <= scan_index) {
        ti->process_before_finish_step();
        return Status::WARN_SCAN_LIMIT;
    }

    // reset cache in cursor
    ti->get_scan_handle().get_ci(handle).reset();
    ti->process_before_finish_step();
    return Status::OK;
}

/**
 * @brief 
 * @param token 
 * @param handle 
 * @param key_read if this is true, this reads key. if not, this reads value.
 * @param buf 
 * @return Status 
 */
Status read_from_scan(Token token, ScanHandle handle, bool key_read,
                      std::string& buf) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    ti->process_before_start_step();

    auto& sh = ti->get_scan_handle();

    /**
     * Check whether the handle is valid.
     */
    if (sh.get_scan_cache().find(handle) == sh.get_scan_cache().end()) {
        ti->process_before_finish_step();
        return Status::WARN_INVALID_HANDLE;
    }

    if (ti->get_read_only()) {
        // todo
        ti->process_before_finish_step();
        return Status::ERR_NOT_IMPLEMENTED;
    }

    scan_handler::scan_elem_type target_elem;
    auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
            sh.get_scan_cache()[handle]);
    std::size_t& scan_index = sh.get_scan_cache_itr()[handle];
    auto itr = scan_buf.begin() + scan_index;
    if (scan_buf.size() <= scan_index) {
        ti->process_before_finish_step();
        return Status::WARN_SCAN_LIMIT;
    }

    /**
     * Check read-own-write
     */
    Record* rec_ptr{const_cast<Record*>(std::get<0>(*itr))}; // NOLINT
    const write_set_obj* inws = ti->get_write_set().search(rec_ptr);
    if (inws != nullptr) {
        if (inws->get_op() == OP_TYPE::DELETE) {
            ti->process_before_finish_step();
            return Status::WARN_ALREADY_DELETE;
        }
        if (key_read) {
            inws->get_key(buf);
        } else {
            inws->get_value(buf);
        }
        ti->process_before_finish_step();
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

#if 0 // todo
    Storage st = std::get<scan_handler::scan_cache_storage_pos>(
            sh.get_scan_cache()[handle]);
    // wp verify
    auto wps = wp::find_wp(storage);
    if (!wp::wp_meta::empty(wps) &&
        wp::wp_meta::find_min_id(wps) != ti->get_batch_id()) {
        abort(ti); // or wait
        /**
         * because: You have to wait for the end of the transaction to read 
         * the prefixed batch write.
         * 
         */
        return Status::ERR_FAIL_WP;
    }
#endif

    if (key_read && sh.get_ci(handle).get_was_read(cursor_info::op_type::key)) {
        // it already read.
        sh.get_ci(handle).get_key(buf);
        ti->process_before_finish_step();
        return Status::OK;
    }
    if (!key_read &&
        sh.get_ci(handle).get_was_read(cursor_info::op_type::value)) {
        // it already read.
        sh.get_ci(handle).get_value(buf);
        ti->process_before_finish_step();
        return Status::OK;
    }

    if (ti->get_tx_type() == TX_TYPE::SHORT) {
        tid_word tidb{};
        std::string valueb{};
        Status rr{};
        if (key_read) {
            const_cast<Record*>(std::get<0>(*itr))->get_key(buf);
            rr = read_record(rec_ptr, tidb, valueb, false);
        } else {
            rr = read_record(rec_ptr, tidb, buf);
        }
        if (rr != Status::OK) {
            ti->process_before_finish_step();
            return rr;
        }
        ti->get_read_set().emplace_back(
                sh.get_scanned_storage_set().get(handle), rec_ptr, tidb);
        if (key_read) {
            sh.get_ci(handle).set_key(buf);
            sh.get_ci(handle).set_was_read(cursor_info::op_type::key);
        } else {
            sh.get_ci(handle).set_value(buf);
            sh.get_ci(handle).set_was_read(cursor_info::op_type::value);
        }

        // create node set info
        auto& ns = ti->get_node_set();
        if (ns.empty() || std::get<1>(ns.back()) != std::get<2>(*itr)) {
            ns.emplace_back(std::get<1>(*itr), std::get<2>(*itr));
        }

        ti->process_before_finish_step();
        return Status::OK;
    } else if (ti->get_tx_type() == TX_TYPE::LONG) {
        version* ver{};
        bool is_latest{false};
        tid_word f_check{};
        auto rc{long_tx::version_function_with_optimistic_check(
                rec_ptr, ti->get_valid_epoch(), ver, is_latest, f_check)};
        if (rc == Status::WARN_NOT_FOUND) {
            return rc;
        }
        if (rc != Status::OK) {
            LOG(ERROR) << "programming error";
            return Status::ERR_FATAL;
        }

        // read latest version after version function
        if (is_latest) {
            if (key_read) {
                rec_ptr->get_key(buf);
                sh.get_ci(handle).set_key(buf);
                sh.get_ci(handle).set_was_read(cursor_info::op_type::key);
            } else {
                ver->get_value(buf);
                sh.get_ci(handle).set_value(buf);
                sh.get_ci(handle).set_was_read(cursor_info::op_type::value);
            }
            if (ver == rec_ptr->get_latest() &&
                loadAcquire(&rec_ptr->get_tidw_ref().get_obj()) ==
                        f_check.get_obj()) {
                // success optimistic read latest version
                return Status::OK;
            }
            /**
              * else: fail to do optimistic read latest version. retry version 
              * function
              */
            long_tx::version_function_without_optimistic_check(
                    ti->get_valid_epoch(), ver);
        }

        // read non-latest version after version function
        if (!key_read) { ver->get_value(buf); }

        if (key_read) {
            sh.get_ci(handle).set_key(buf);
            sh.get_ci(handle).set_was_read(cursor_info::op_type::key);
        } else {
            sh.get_ci(handle).set_value(buf);
            sh.get_ci(handle).set_was_read(cursor_info::op_type::value);
        }
        return Status::OK;
    }
    LOG(ERROR) << "programming error";
    return Status::ERR_NOT_IMPLEMENTED;
}

Status read_key_from_scan(Token const token, ScanHandle const handle,
                          std::string& key) {
    return read_from_scan(token, handle, true, key);
}

Status read_value_from_scan(Token const token, ScanHandle const handle,
                            std::string& value) {
    return read_from_scan(token, handle, false, value);
}

[[maybe_unused]] Status scannable_total_index_size(Token const token,
                                                   ScanHandle const handle,
                                                   std::size_t& size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    ti->process_before_start_step();

    auto& sh = ti->get_scan_handle();

    if (sh.get_scan_cache().find(handle) == sh.get_scan_cache().end()) {
        /**
         * the handle was invalid.
         */
        ti->process_before_finish_step();
        return Status::WARN_INVALID_HANDLE;
    }

    size = std::get<scan_handler::scan_cache_vec_pos>(
                   sh.get_scan_cache()[handle])
                   .size();
    ti->process_before_finish_step();
    return Status::OK;
}

} // namespace shirakami
