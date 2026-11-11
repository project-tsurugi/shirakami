
#include <mutex>

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

static Status next_check_not_found(session*, Storage, Record*);
static void check_ltx_scan_range_ep_and_log(session*, scan_context*, std::string_view);

static Status next_vscan(session* ti, scan_context* sc) {
    // increment cursor
    Storage st = sc->get_storage();
    for (;;) {
        auto& scv = sc->get_context_vscan_ref();
        auto& scan_index = sc->get_scan_index_ref();
        ++scan_index;

        auto& scan_buf = scv.get_vec();
        // check range of cursor
        if (scan_buf.size() <= scan_index) {
            scan_index = scan_buf.size(); // stop at scan_buf.size
            if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
                auto last_key_view = std::get<0>(scan_buf.back())->get_key_view();
                check_ltx_scan_range_ep_and_log(ti, sc, last_key_view);
            }
            return Status::WARN_SCAN_LIMIT;
        }

        // check target record
        auto itr = scan_buf.begin() + scan_index; // NOLINT
        Record* rec_ptr = const_cast<Record*>(std::get<0>(*itr)); // NOLINT

        auto rc = next_check_not_found(ti, st, rec_ptr);
        if (rc == Status::OK) { break; }
        if (rc == Status::INTERNAL_WARN_NOT_FOUND) { continue; }
        return rc;
    }

    // reset cache in cursor
    //ti->get_scan_handle().get_ci(handle).reset();
    return Status::OK;
}

static Status next_iscan(session* ti, scan_context* sc) {
    // increment cursor
    Storage st = sc->get_storage();
    auto occ_cb = [&ti](yakushima::node_version64* nvp, yakushima::node_version64_body nvb) -> bool {
        auto rc = ti->get_node_set().emplace_back({nvb, nvp});
        return (rc == Status::ERR_CC);
    };
    auto& sci = sc->get_context_iscan_ref();
    if (sci.get_error() == Status::ERR_CC) {
        // TODO: return ERR_CC
        // but currently next() cannot return ERR_CC by API spec.
        // avoid infinite loop when user repeat calling next() without read_*_from_scan().
        sci.set_max_size(sc->get_scan_index());
        return Status::WARN_SCAN_LIMIT;
    }
    for (;;) {
        auto& scan_index = sc->get_scan_index_ref();
        ++scan_index;

        // check range of cursor
        if (sci.get_max_size() <= scan_index) {
            return Status::WARN_SCAN_LIMIT;
        }

        yakushima::status yrc{};
        void* value{};
        if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
            yrc = yakushima::iscan_next(sci.get_ycontext_ref(), value, occ_cb);
        } else {
            yrc = yakushima::iscan_next(sci.get_ycontext_ref(), value);
        }

        // check target record
        if (yrc == yakushima::status::WARN_CONCURRENT_OPERATIONS || yrc == yakushima::status::WARN_ABORTED_BY_USER) {
            sci.set_error(Status::ERR_CC);
            break;
        }
        if (yrc == yakushima::status::OK_SCAN_END) {
            Record* rec_ptr = sci.get_rec_ptr_ref();
            sci.set_max_size(scan_index);
            if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
                check_ltx_scan_range_ep_and_log(ti, sc, rec_ptr->get_key_view());
            }
            return Status::WARN_SCAN_LIMIT;
        }
        Record* rec_ptr = reinterpret_cast<Record*>(value); // NOLINT
        sci.get_rec_ptr_ref() = rec_ptr;

        auto rc = next_check_not_found(ti, st, rec_ptr);
        if (rc == Status::OK) { break; }
        if (rc == Status::INTERNAL_WARN_NOT_FOUND) { continue; }
        return rc;
    }

    // reset cache in cursor
    //ti->get_scan_handle().get_ci(handle).reset();
    return Status::OK;
}

// XXX: consider merge with check_not_found() (called from open_scan())
static Status next_check_not_found(session* ti, Storage st, Record* rec_ptr) {
    write_set_obj* inws{};
    if (ti->get_tx_type() != transaction_options::transaction_type::READ_ONLY) {
        // check local write set
        inws = ti->get_write_set().search(rec_ptr);
        if (inws != nullptr) {
            /**
             * If it exists and it is not delete operation, read from scan api
             * call should be able to read the record.
             */
            if (inws->get_op() == OP_TYPE::DELETE) { return Status::INTERNAL_WARN_NOT_FOUND; }
            return Status::OK;
        }
    }
    // not in local write set

    tid_word tid{loadAcquire(rec_ptr->get_tidw().get_obj())};
    if (!tid.get_absent()) {
        // normal page
        if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
            return Status::OK;
        }
        if (ti->get_tx_type() == transaction_options::transaction_type::LONG ||
            ti->get_tx_type() == transaction_options::transaction_type::READ_ONLY) {
            if (tid.get_epoch() < ti->get_valid_epoch()) { return Status::OK; }
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
                return Status::OK;
            }
        } else {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return Status::ERR_FATAL;
        }
    } else if (tid.get_latest()) {
        // inserting page
        // read own inserting check is already done

        // short tx should read inserting page
        if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
            return Status::OK;
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
                    if (ver->get_tid().get_epoch() < ti->get_valid_epoch()) {
                        break;
                    }
                }
                if (ver != nullptr) {
                    // there is a readable rec
                    return Status::OK;
                }
            } else {
                // it can read latest version
                return Status::OK;
            }
        }
    } else {
        // absent && not latest == deleted
        if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
            /**
             * short mode must read deleted record and verify, so add read set
             */
            ti->push_to_read_set_for_stx({st, rec_ptr, tid});
        }
        if (ti->get_tx_type() == transaction_options::transaction_type::LONG ||
            ti->get_tx_type() == transaction_options::transaction_type::READ_ONLY) {
            if (tid.get_epoch() >= ti->get_valid_epoch()) {
                // there may be readable rec
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
                    return Status::OK;
                }
            }
        }
    }
    return Status::INTERNAL_WARN_NOT_FOUND;
}

/**
 * @pre This is called by only long tx mode
 * @brief register right or left end point info
 */
static void check_ltx_scan_range_ep_and_log(session* ti, scan_context* sc, std::string_view last_key) {
    Storage st = sc->get_storage();
    // get storage info
    wp::wp_meta* wp_meta_ptr{};
    if (wp::find_wp_meta(st, wp_meta_ptr) != Status::OK) {
        // todo special case. interrupt DDL
        return;
    }
    if (sc->get_right_to_left()) {
        long_tx::update_local_read_range(ti, wp_meta_ptr,
                                         sc->get_end_key(), sc->get_end_kind(),
                                         last_key, scan_endpoint::INCLUSIVE);
    } else {
        long_tx::update_local_read_range(ti, wp_meta_ptr,
                                         last_key, scan_endpoint::INCLUSIVE,
                                         sc->get_end_key(), sc->get_end_kind());
    }
}

static Status next_iscan_from_prefetch(session* ti, scan_context* sc) {
    // increment cursor
    Storage st = sc->get_storage();
    auto& sci = sc->get_context_iscan_ref();
    auto& prefetched = sci.get_scan_prefetch_ref();
    auto occ_cb = [&ti](yakushima::node_version64* nvp, yakushima::node_version64_body nvb) -> bool {
        auto rc = ti->get_node_set().emplace_back({nvb, nvp});
        return (rc == Status::ERR_CC);
    };
    if (sci.get_error() == Status::ERR_CC) {
        // TODO: return ERR_CC
        // but currently next() cannot return ERR_CC by API spec.
        // avoid infinite loop when user repeat calling next() without read_*_from_scan().
        sci.set_max_size(sc->get_scan_index());
        return Status::WARN_SCAN_LIMIT;
    }
    for (;;) {
        auto& scan_index = sc->get_scan_index_ref();
        ++scan_index;

        // check range of cursor
        if (sci.get_max_size() <= scan_index) { // already reached limit before
            return Status::WARN_SCAN_LIMIT;
        }
        if (prefetched.empty()) { // prefetch-seq is end by rec_ptr (by scan_max)
            sci.set_max_size(scan_index);
            return Status::WARN_SCAN_LIMIT;
        }

        bool fail = prefetched.consume_node_block(occ_cb); // if not-SHORT, node_block is always empty
        if (fail) {
            sci.set_error(Status::ERR_CC);
            prefetched.discard();
            break;
        }
        if (prefetched.empty()) { // prefetch-seq is end by node-block. i.e. read till end-range
            sci.set_max_size(scan_index);
            if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
                Record* rec_ptr = sci.get_rec_ptr_ref(); // last-read key
                check_ltx_scan_range_ep_and_log(ti, sc, rec_ptr->get_key_view());
            }
            return Status::WARN_SCAN_LIMIT;
        }
        Record* rec_ptr = prefetched.get_and_pop_front_rec_ptr();
        sci.get_rec_ptr_ref() = rec_ptr;

        Status rc = next_check_not_found(ti, st, rec_ptr);
        if (rc == Status::OK) { break; }
        if (rc == Status::INTERNAL_WARN_NOT_FOUND) { continue; }
        return rc;
    }

    return Status::OK;
}

static Status next_body(Token const token, ScanHandle const handle) { // NOLINT
    auto* ti = static_cast<session*>(token);
    auto* sc = static_cast<scan_context*>(handle);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_handle().check_valid_scan_handle(sc) != Status::OK) {
        return Status::WARN_INVALID_HANDLE;
    }
    // valid handle

    if (sc->is_iscan()) {
        auto& sci = sc->get_context_iscan_ref();
        if (sci.is_prefetched()) { return next_iscan_from_prefetch(ti, sc); }
        return next_iscan(ti, sc);
    } else { // NOLINT
        return next_vscan(ti, sc);
    }
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

static Status prefetch_iscan_body(Token token, ScanHandle handle, std::size_t max_size) {
    auto* ti = static_cast<session*>(token);
    auto* sc = static_cast<scan_context*>(handle);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    // Check whether the handle is valid.
    if (ti->get_scan_handle().check_valid_scan_handle(sc) != Status::OK) {
        return Status::WARN_INVALID_HANDLE;
    }
    if (!sc->is_iscan()) { return Status::WARN_ILLEGAL_OPERATION; }

    auto& sci = sc->get_context_iscan_ref();
    if (sci.is_prefetched()) { return Status::WARN_ILLEGAL_OPERATION; }

    auto& prefetched = sci.create_scan_prefetch();
    std::size_t i = 0;
    for (; i < max_size; i++) {
        auto& block_head = prefetched.begin_node_block();
        yakushima::status yrc{};
        void* value{};
        if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
            auto occ_cb = [&prefetched, &block_head]
                          (yakushima::node_version64* nvp, yakushima::node_version64_body nvb) -> bool {
                prefetched.push_back_node(nvb, nvp);
                block_head++;
                return false;
            };
            yrc = yakushima::iscan_next(sci.get_ycontext_ref(), value, occ_cb);
        } else {
            yrc = yakushima::iscan_next(sci.get_ycontext_ref(), value);
        }

        if (yrc == yakushima::status::WARN_CONCURRENT_OPERATIONS || yrc == yakushima::status::WARN_ABORTED_BY_USER) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            sci.set_error(Status::ERR_CC);
            prefetched.discard();
            return Status::ERR_FATAL;
        }
        if (yrc == yakushima::status::OK_SCAN_END) {
            break;
        }
        Record* rec_ptr = reinterpret_cast<Record*>(value); // NOLINT
        prefetched.push_back_rec_ptr(rec_ptr);
    }
    return Status::OK;
}

Status prefetch_iscan(Token token, ScanHandle handle, std::size_t max_size) {
    shirakami_log_entry << "prefetch_iscan, token: " << token << ", handle: " << handle
                        << ", max_size: " << max_size;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term(), std::defer_lock};
        if (ti->get_mutex_flags().do_readaccess_daterm()) { lock.lock(); }
        ret = prefetch_iscan_body(token, handle, max_size);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "prefetch_iscan, Status: " << ret;
    return ret;
}

} // namespace shirakami
