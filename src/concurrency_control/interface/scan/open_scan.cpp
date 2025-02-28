
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

inline Status find_open_scan_slot(session* const ti, // NOLINT
                                  ScanHandle& out) {
    auto& sh = ti->get_scan_handle();
    if (auto* i = sh.get_scan_cache().allocate(); i != nullptr) {
            out = i;
            // clear cursor info
            sh.get_scan_cache()[i].get_scan_index() = 0;
            return Status::OK;
    }
    return Status::WARN_MAX_OPEN_SCAN;
}

// TODO: create a new header file and move this function definition there as constexpr
Status check_empty_scan_range(const std::string_view l_key, const scan_endpoint l_end,
                              const std::string_view r_key, const scan_endpoint r_end) {
    if (r_end == scan_endpoint::INF) {
        return Status::OK; // if right end is inf, not empty
    }
    if (l_end == scan_endpoint::INF) {
        // if left end is inf, not empty in most cases
        if (r_end == scan_endpoint::EXCLUSIVE && r_key.empty()) {
            return Status::WARN_NOT_FOUND; // the only exception
        }
        return Status::OK;
    }
    int cmp_key = l_key.compare(r_key);
    if (cmp_key < 0) { return Status::OK; } // if left is less, not empty
    if (cmp_key > 0) { return Status::WARN_NOT_FOUND; } // if left is greater, invalid range
    // l_key == r_key
    return (l_end == scan_endpoint::INCLUSIVE && r_end == scan_endpoint::INCLUSIVE)
            ? Status::OK // single point, not empty
            : Status::WARN_NOT_FOUND; // empty
}

/**
 * This is for some creating for this tx and consider other concurrent strand
 * thread. If that failed, this cleanup local effect, respect the result and
 * return;
 */
Status fin_process(session* const ti, Status const this_result) {
    if (this_result <= Status::OK) {
        // It is not error by this strand thread, check termination
        std::unique_lock<std::mutex> lk{ti->get_mtx_termination()};
        transaction_options::transaction_type this_tx_type{ti->get_tx_type()};
        if (ti->get_result_info().get_reason_code() != reason_code::UNKNOWN) {
            // but concurrent strand thread failed
            if (this_tx_type == transaction_options::transaction_type::LONG) {
                long_tx::abort(ti);
                return Status::ERR_CC;
            }
            if (this_tx_type == transaction_options::transaction_type::SHORT) {
                short_tx::abort(ti);
                return Status::ERR_CC;
            }
        }
    }

    return this_result;
}

/**
 * @brief
 *
 * @param ti
 * @param st
 * @param scan_res
 * @param head_skip_rec_n
 * @return Status::OK
 * @return Status::WARN_NOT_FOUND
 */
Status check_not_found(
        session* ti, Storage st,
        std::vector<std::tuple<std::string, Record**, std::size_t>>& scan_res,
        std::size_t& head_skip_rec_n) {
    head_skip_rec_n = 0;
    bool once_not_skip{false};
    for (auto& elem : scan_res) {
        Record* rec_ptr{reinterpret_cast<Record*>(std::get<1>(elem))}; // NOLINT
        // by inline optimization
        tid_word tid{loadAcquire(rec_ptr->get_tidw().get_obj())};
        if (!tid.get_absent()) {
            // normal page.
            if (ti->get_tx_type() ==
                transaction_options::transaction_type::SHORT) {
                return Status::OK;
            }
            if (ti->get_tx_type() ==
                        transaction_options::transaction_type::LONG ||
                ti->get_tx_type() ==
                        transaction_options::transaction_type::READ_ONLY) {
                if (tid.get_epoch() < ti->get_valid_epoch()) {
                    // latest version check
                    return Status::OK;
                }
                version* ver = rec_ptr->get_latest();
                for (;;) {
                    ver = ver->get_next();
                    if (ver == nullptr) { break; }
                    if (ver->get_tid().get_epoch() < ti->get_valid_epoch()) {
                        if (ver->get_tid().get_absent()) { break; }
                        return Status::OK;
                    }
                }
            } else {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "unreachable path";
                return Status::ERR_FATAL;
            }
        } else if (tid.get_latest()) {
            // inserting page.
            // short tx read inserting page.
            if (ti->get_tx_type() ==
                transaction_options::transaction_type::SHORT) {
                return Status::OK;
            }

            // check read own write
            write_set_obj* inws = ti->get_write_set().search(rec_ptr);
            if (inws != nullptr) {
                if (inws->get_op() == OP_TYPE::INSERT ||
                    inws->get_op() == OP_TYPE::UPSERT) {
                    return Status::OK;
                }
            }

            /**
             * first version must be inserting page or deleted page
             */
            version* ver = rec_ptr->get_latest();
            for (;;) {
                ver = ver->get_next();
                if (ver == nullptr) { break; }
                if (ver->get_tid().get_epoch() < ti->get_valid_epoch()) {
                    if (ver->get_tid().get_absent()) { break; }
                    return Status::OK;
                }
            }
        } else {
            // absent && not latest == deleted
            if (ti->get_tx_type() ==
                transaction_options::transaction_type::SHORT) {
                /**
                 * short mode must read deleted record and verify, so add read set
                 */
                ti->push_to_read_set_for_stx({st, rec_ptr, tid});
            }
            if (ti->get_tx_type() ==
                        transaction_options::transaction_type::LONG ||
                ti->get_tx_type() ==
                        transaction_options::transaction_type::READ_ONLY) {
                if (tid.get_epoch() >= ti->get_valid_epoch()) {
                    // there may be readable rec
                    version* ver = rec_ptr->get_latest();
                    for (;;) {
                        ver = ver->get_next();
                        if (ver == nullptr) { break; }
                        if (ver->get_tid().get_epoch() <
                            ti->get_valid_epoch()) {
                            if (ver->get_tid().get_absent()) { break; }
                            return Status::OK;
                        }
                    }
                }
            }
        }
        if (!once_not_skip) { ++head_skip_rec_n; }
    }
    return Status::WARN_NOT_FOUND;
}

Status open_scan_body(Token const token, Storage storage, // NOLINT
                      const std::string_view l_key, const scan_endpoint l_end,
                      const std::string_view r_key, const scan_endpoint r_end,
                      ScanHandle& handle, std::size_t const max_size,
                      bool right_to_left) {
    // check constraint: key
    auto ret = check_constraint_key_length(l_key);
    if (ret != Status::OK) { return ret; }
    ret = check_constraint_key_length(r_key);
    if (ret != Status::OK) { return ret; }

    // take thread info
    auto* ti = static_cast<session*>(token);
    // tx begin if not
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    // pre-process

    // check about long tx
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG ||
        ti->get_tx_type() == transaction_options::transaction_type::READ_ONLY) {
        // check start epoch
        if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
            return Status::WARN_PREMATURE;
        }
        // check high priori tx
        if (ti->find_high_priority_short(false) == Status::WARN_PREMATURE) {
            return Status::WARN_PREMATURE;
        }
        // check for read area invalidation
        auto rs = long_tx::check_read_area(ti, storage);
        if (rs == Status::ERR_READ_AREA_VIOLATION) {
            long_tx::abort(ti);
            std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
            ti->get_result_info().set_storage_name(storage);
            ti->set_result(reason_code::CC_LTX_READ_AREA_VIOLATION);
            return rs;
        }
    }

    // check storage existence
    wp::wp_meta* wp_meta_ptr{};
    if (wp::find_wp_meta(storage, wp_meta_ptr) != Status::OK) {
        return Status::WARN_STORAGE_NOT_FOUND;
    }

    // ==========
    // wp verify section
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        /**
         * early abort optimization. If it is not, it finally finds at commit phase.
         */
        auto wps = wp::find_wp(storage);
        auto find_min_ep{wp::wp_meta::find_min_ep(wps)};
        if (find_min_ep != 0 && find_min_ep <= ti->get_step_epoch()) {
            short_tx::abort(ti);
            std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
            ti->get_result_info().set_storage_name(storage);
            ti->set_result(reason_code::CC_OCC_WP_VERIFY);
            return Status::ERR_CC;
        }
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::LONG) {
        // wp verify and forwarding
        long_tx::wp_verify_and_forwarding(ti, wp_meta_ptr);
    } else if (ti->get_tx_type() !=
               transaction_options::transaction_type::READ_ONLY) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }
    // ==========

    // check read information
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        wp::page_set_meta* psm{};
        auto rc = wp::find_page_set_meta(storage, psm);
        if (rc != Status::OK) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return Status::ERR_FATAL;
        }
        range_read_by_long* rrbp{psm->get_range_read_by_long_ptr()};
        /**
         * register read_by_set
         * todo: enhancement:
         * The range is modified according to the execution of
         * read_from_scan, and the range is fixed and registered at the end of
         * the transaction.
         */
        ti->get_range_read_set_for_ltx().insert(std::make_tuple(
                rrbp, std::string(l_key), l_end, std::string(r_key), r_end));
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::SHORT) {
        wp::page_set_meta* psm{};
        auto rc{wp::find_page_set_meta(storage, psm)};
        if (rc == Status::WARN_NOT_FOUND) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return Status::ERR_FATAL;
        }
        range_read_by_short* rrbs{psm->get_range_read_by_short_ptr()};
        ti->push_to_range_read_by_short_set(rrbs);
    } else if (ti->get_tx_type() !=
               transaction_options::transaction_type::READ_ONLY) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }

    // for no hit
    auto update_local_read_range_if_ltx = [ti, wp_meta_ptr, l_key, l_end, r_key,
                                           r_end]() {
        if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
            long_tx::update_local_read_range(ti, wp_meta_ptr, l_key, l_end,
                                             r_key, r_end);
        }
    };

    // scan for index
    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    constexpr std::size_t index_rec_ptr{1};
    std::vector<std::pair<yakushima::node_version64_body,
                          yakushima::node_version64*>>
            nvec;
    constexpr std::size_t index_nvec_body{0};
    constexpr std::size_t index_nvec_ptr{1};
    auto rc = check_empty_scan_range(l_key, l_end, r_key, r_end);

    if (rc == Status::OK) {
        rc = scan(storage, l_key, l_end, r_key, r_end, max_size, scan_res, &nvec, right_to_left);
    }
    if (rc != Status::OK) {
        update_local_read_range_if_ltx();
        return rc;
    }
    // not empty of targeting records

    std::size_t head_skip_rec_n{};
    /**
     * skip leading unreadable records.
     */
    rc = check_not_found(ti, storage, scan_res, head_skip_rec_n);
    if (rc != Status::OK) {
        /**
         * The fact must be guaranteed by isolation. So it can get node version
         * and it must check about phantom at commit phase.
         */
        {
            if (ti->get_tx_type() ==
                transaction_options::transaction_type::SHORT) {
                for (auto&& elem : nvec) {
                    auto rc_ns = ti->get_node_set().emplace_back(elem);
                    if (rc_ns == Status::ERR_CC) {
                        short_tx::abort(ti);
                        ti->get_result_info().set_storage_name(storage);
                        ti->set_result(reason_code::CC_OCC_PHANTOM_AVOIDANCE);
                        return Status::ERR_CC;
                    }
                }
            }
        }
        update_local_read_range_if_ltx();
        return fin_process(ti, rc);
    }

    /**
     * You must ensure that new elements are not interrupted in the range at
     * the node that did not retrieve the element but scanned it when masstree
     * scanned it.
     */
    std::size_t nvec_delta{0};
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        if (scan_res.size() < nvec.size()) {
            auto add_ns = [&ti, &nvec, storage](std::size_t n) {
                for (std::size_t i = 0; i < n; ++i) {
                    auto rc = ti->get_node_set().emplace_back(nvec.at(i));
                    if (rc == Status::ERR_CC) {
                        short_tx::abort(ti);
                        std::unique_lock<std::mutex> lk{
                                ti->get_mtx_result_info()};
                        ti->get_result_info().set_storage_name(storage);
                        ti->set_result(reason_code::CC_OCC_PHANTOM_AVOIDANCE);
                        return Status::ERR_CC;
                    }
                }
                return Status::OK;
            };
            if (scan_res.size() + 1 == nvec.size()) {
                nvec_delta = 1;
                rc = add_ns(1);
                if (rc == Status::ERR_CC) { return rc; }


            } else if (scan_res.size() + 2 == nvec.size()) {
                nvec_delta = 2;
                rc = add_ns(2);
                if (rc == Status::ERR_CC) { return rc; }
            }
        }
    }

    // for hit, register left end point info as already read
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        long_tx::update_local_read_range(ti, wp_meta_ptr, l_key, l_end);
    }

    // Cache a pointer to record.
    auto& sh = ti->get_scan_handle();
    {
        // lock for strand
        std::lock_guard<std::shared_mutex> lk{sh.get_mtx_scan_cache()};

        // find slot to log scan result.
        auto rc = find_open_scan_slot(ti, handle);
        if (rc != Status::OK) { return rc; }

        sh.get_scan_cache()[handle].get_storage() = storage;
        auto& vec = sh.get_scan_cache()[handle].get_vec();
        vec.reserve(scan_res.size());
        for (std::size_t i = 0; i < scan_res.size(); ++i) {
            vec.emplace_back(reinterpret_cast<Record*>( // NOLINT
                                     std::get<index_rec_ptr>(scan_res.at(i))),
                             // by inline optimization
                             std::get<index_nvec_body>(nvec.at(i + nvec_delta)),
                             std::get<index_nvec_ptr>(nvec.at(i + nvec_delta)));
        }

        // increment for head skipped records
        // may need mutex for strand
        std::size_t& scan_index = ti->get_scan_handle().get_scan_cache()[handle].get_scan_index();
        scan_index += head_skip_rec_n;

        sh.get_scanned_storage_set().set(handle, storage);
        sh.set_r_key(r_key);
        sh.set_r_end(r_end);
    }
    return fin_process(ti, Status::OK);
}

Status open_scan(Token const token, Storage storage, // NOLINT
                 const std::string_view l_key, const scan_endpoint l_end,
                 const std::string_view r_key, const scan_endpoint r_end,
                 ScanHandle& handle, std::size_t const max_size, bool right_to_left) {
    shirakami_log_entry_lazy("open_scan token: " << token << ", storage: " << storage
                             << shirakami_binstring(l_key) << ", l_end: " << l_end
                             << shirakami_binstring(r_key) << ", r_end: " << r_end
                             << ", max_size: " << max_size << ", right_to_left: " << right_to_left);
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term()};
        ret = open_scan_body(token, storage, l_key, l_end, r_key, r_end, handle,
                             max_size, right_to_left);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "open_scan, Status: " << ret << ", handle: " << handle;
    return ret;
}

} // namespace shirakami
