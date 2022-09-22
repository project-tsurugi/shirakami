
#include <algorithm>
#include <string_view>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

#include "index/yakushima/include/interface.h"

#include "glog/logging.h"

namespace shirakami::long_tx {

// ==============================
// static inline functions for this source
static inline void cancel_flag_inserted_records(session* const ti) {
    auto process = [ti](std::pair<Record* const, write_set_obj>& wse) {
        auto&& wso = std::get<1>(wse);
        if (wso.get_op() == OP_TYPE::INSERT) {
            auto* rec_ptr = std::get<0>(wse);
            tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
            // pre-check
            if (check.get_latest() && check.get_absent()) {
                rec_ptr->get_tidw_ref().lock();
                check = loadAcquire(rec_ptr->get_tidw_ref().get_obj());
                // main-check
                if (check.get_latest() && check.get_absent()) {
                    tid_word tid{};
                    tid.set_absent(true);
                    tid.set_latest(false);
                    tid.set_lock(false);
                    tid.set_epoch(ti->get_valid_epoch());
                    rec_ptr->set_tid(tid); // and unlock
                } else {
                    rec_ptr->get_tidw_ref().unlock();
                }
            }
        }
    };

    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        process(wso);
    }
}

static inline void compute_tid(session* ti, tid_word& ctid) {
    ctid.set_epoch(ti->get_valid_epoch());
    ctid.set_lock(false);
    ctid.set_absent(false);
    ctid.set_latest(true);
}

static inline void create_tombstone_if_need(session* const ti,
                                            write_set_obj& wso) {
    std::string key{};
    wso.get_rec_ptr()->get_key(key);
RETRY: // NOLINT
    Record* rec_ptr{};
    auto rc = get<Record>(wso.get_storage(), key, rec_ptr);
    if (rc == Status::OK) {
        // hit in index
        if (wso.get_rec_ptr() != rec_ptr) { wso.set_rec_ptr(rec_ptr); }
        /**
          * not change from read phase
          * In ltx view, No tx interrupt this. GC thread may interrupt.
          * Because high priori tx is 
          * nothing. In other words, this commit waited for high priori txs.
          * so it don't need lock record.
          * check ts
          */
        rec_ptr->get_tidw_ref().lock();
        tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        if ((check.get_latest() && check.get_absent()) || !check.get_absent()) {
            // ok
            rec_ptr->get_tidw_ref().unlock();
            return;
        }
        // it is deleted state, change it to inserting state.
        check.set_latest(true);
        rec_ptr->set_tid(check);
        // check it is hooked yet
        auto rc = get<Record>(wso.get_storage(), key, rec_ptr);
        auto cleanup_old_process = [&wso](tid_word check) {
            check.set_latest(false);
            check.set_absent(true);
            check.set_lock(false);
            wso.get_rec_ptr()->set_tid(check);
        };
        if (rc == Status::OK) {
            // some key hit
            if (wso.get_rec_ptr() == rec_ptr) {
                // success converting deleted to inserted
                rec_ptr->get_tidw_ref().unlock();
                return;
            }
            // converting record was unhooked by gc
            cleanup_old_process(check);
            goto RETRY; // NOLINT
        } else {
            // no key hit
            // gc interrupted
            cleanup_old_process(check);
            goto RETRY; // NOLINT
        }
    } else {
        // no key hit
        rec_ptr = new Record(key); // NOLINT
        tid_word tid = loadAcquire(rec_ptr->get_tidw_ref().get_obj());
        tid.set_latest(true);
        tid.set_absent(true);
        tid.set_lock(true);
        rec_ptr->set_tid(tid);
        yakushima::node_version64* nvp{};
        if (yakushima::status::OK == put<Record>(ti->get_yakushima_token(),
                                                 wso.get_storage(), key,
                                                 rec_ptr, nvp)) {
            // success inserting
            wso.set_rec_ptr(rec_ptr);
            return;
        }
        // else insert_result == Status::WARN_ALREADY_EXISTS
        // so retry from index access
        delete rec_ptr; // NOLINT
        goto RETRY;     // NOLINT
    }
}

static inline void expose_local_write(session* ti) {
    tid_word ctid{};
    compute_tid(ti, ctid);

    auto process = [ti](std::pair<Record* const, write_set_obj>& wse,
                        tid_word ctid) {
        auto* rec_ptr = std::get<0>(wse);
        auto&& wso = std::get<1>(wse);
        [[maybe_unused]] bool should_log{true};
        switch (wso.get_op()) {
            case OP_TYPE::UPSERT: {
                // not accept fallthrough!
                // create tombstone if need.
                create_tombstone_if_need(ti, wso);
                [[fallthrough]];
            }
            case OP_TYPE::INSERT: {
                tid_word tid{rec_ptr->get_tidw_ref().get_obj()};
                if (tid.get_latest() && tid.get_absent()) {
                    // update value
                    std::string vb{};
                    wso.get_value(vb);
                    rec_ptr->get_latest()->set_value(vb);
                    // unlock and set ctid
                    rec_ptr->set_tid(ctid);
                    break;
                }
                [[fallthrough]]; // upsert is update
            }
            case OP_TYPE::DELETE: {
                if (wso.get_op() == OP_TYPE::DELETE) { // for fallthrough
                    ctid.set_latest(false);
                    ctid.set_absent(true);
                }
                [[fallthrough]];
            }
            case OP_TYPE::UPDATE: {
                // lock record
                rec_ptr->get_tidw_ref().lock();
                tid_word pre_tid{rec_ptr->get_tidw_ref().get_obj()};

                if (ti->get_valid_epoch() > pre_tid.get_epoch()) {
                    // case: first of list
                    std::string vb{};
                    if (wso.get_op() != OP_TYPE::DELETE) { wso.get_value(vb); }
                    version* new_v{new version( // NOLINT
                            vb, rec_ptr->get_latest())};
                    // prepare tid for old version
                    pre_tid.set_absent(false);
                    pre_tid.set_latest(false);
                    pre_tid.set_lock(false);
                    // set old version tid
                    rec_ptr->get_latest()->set_tid(pre_tid);
                    // set latest
                    rec_ptr->set_latest(new_v);
                    // unlock and set ctid
                    rec_ptr->set_tid(ctid);
                } else if (ti->get_valid_epoch() == pre_tid.get_epoch()) {
                    // para write
                    should_log = false;
                    ctid.set_tid(pre_tid.get_tid());
                    ctid.set_epoch(pre_tid.get_epoch());
                    rec_ptr->set_tid(ctid);
                } else {
                    // case: middle of list
                    auto version_creation = [&wso, ctid](version* pre_ver,
                                                         version* ver) {
                        std::string vb{};
                        if (wso.get_op() != OP_TYPE::DELETE) {
                            // load payload if not delete.
                            wso.get_value(vb);
                        }
                        version* new_v{new version(ctid, vb, ver)};
                        pre_ver->set_next(new_v);
                    };
                    should_log = false;
                    version* pre_ver{rec_ptr->get_latest()};
                    version* ver{rec_ptr->get_latest()->get_next()};
                    for (;;) {
                        if (ver == nullptr) {
                            // version creation
                            version_creation(pre_ver, ver);
                            break;
                        }
                        // checking version exist. check tid.
                        tid_word tid{ver->get_tid()};
                        if (tid.get_epoch() < ti->get_valid_epoch()) {
                            // version creation
                            version_creation(pre_ver, ver);
                            break;
                        }
                        if (tid.get_epoch() == ti->get_valid_epoch()) {
                            // para (partial order) write, invisible write
                            break;
                        }
                        pre_ver = ver;
                        ver = ver->get_next();
                    }
                    rec_ptr->get_tidw_ref().unlock();
                }
                break;
            }
            default: {
                LOG(FATAL) << "unknown operation type.";
                break;
            }
        }
#ifdef PWAL
        if (should_log) {
            // add log records to local wal buffer
            std::string key{};
            wso.get_rec_ptr()->get_key(key);
            std::string val{};
            wso.get_value(val);
            log_operation lo{};
            switch (wso.get_op()) {
                case OP_TYPE::INSERT: {
                    lo = log_operation::INSERT;
                    break;
                }
                case OP_TYPE::UPDATE: {
                    lo = log_operation::UPDATE;
                    break;
                }
                case OP_TYPE::UPSERT: {
                    lo = log_operation::UPSERT;
                    break;
                }
                case OP_TYPE::DELETE: {
                    lo = log_operation::DELETE;
                    break;
                }
                default: {
                    LOG(ERROR) << "programming error";
                    return Status::ERR_FATAL;
                }
            }
            ti->get_lpwal_handle().push_log(shirakami::lpwal::log_record(
                    lo,
                    lpwal::write_version_type(ti->get_valid_epoch(),
                                              ti->get_long_tx_id()),
                    wso.get_storage(), key, val));
        }
#endif
        return Status::OK;
    };

#ifdef PWAL
    std::unique_lock<std::mutex> lk{ti->get_lpwal_handle().get_mtx_logs()};
#endif
    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        process(wso, ctid);
    }
}

static inline void register_wp_result_and_remove_wps(session* ti,
                                                     const bool was_committed) {
    for (auto&& elem : ti->get_wp_set()) {
        Storage storage = elem.first;
        Storage page_set_meta_storage = wp::get_page_set_meta_storage();
        std::string_view page_set_meta_storage_view = {
                reinterpret_cast<char*>( // NOLINT
                        &page_set_meta_storage),
                sizeof(page_set_meta_storage)};
        std::string_view storage_view = {
                reinterpret_cast<char*>(&storage), // NOLINT
                sizeof(storage)};
        std::pair<wp::page_set_meta**, std::size_t> out{};
        auto rc{yakushima::get<wp::page_set_meta*>(page_set_meta_storage_view,
                                                   storage_view, out)};
        if (rc != yakushima::status::OK) {
            LOG(ERROR) << "programing error: " << rc;
            return;
        }
        if (Status::OK != (*out.first)
                                  ->get_wp_meta_ptr()
                                  ->register_wp_result_and_remove_wp(
                                          std::make_tuple(ti->get_valid_epoch(),
                                                          ti->get_long_tx_id(),
                                                          was_committed))) {
            LOG(FATAL);
        }
    }
}

static inline void cleanup_process(session* const ti,
                                   const bool was_committed) {
    // global effect
    register_wp_result_and_remove_wps(ti, was_committed);
    ongoing_tx::remove_id(ti->get_long_tx_id());

    // local effect
    ti->clean_up();
}

// ==============================

// ==============================
// functions declared at header
Status abort(session* const ti) { // NOLINT
    cancel_flag_inserted_records(ti);

    // about tx state
    // this should before clean up
    ti->set_tx_state_if_valid(TxState::StateKind::ABORTED);

    // clean up
    cleanup_process(ti, false);
    return Status::OK;
}

void register_read_by(session* const ti) {
    // point read
    for (auto&& elem : ti->get_point_read_by_long_set()) {
        elem->push({ti->get_valid_epoch(), ti->get_long_tx_id()});
    }

    // range read
    for (auto&& elem : ti->get_range_read_by_long_set()) {
        std::get<0>(elem)->push({ti->get_valid_epoch(), ti->get_long_tx_id(),
                                 std::get<1>(elem), std::get<2>(elem),
                                 std::get<3>(elem), std::get<4>(elem)});
    }
}

Status verify_read_by(session* const ti) {
    auto this_epoch = ti->get_valid_epoch();

    // forwarding verify
    auto gc_threshold = ongoing_tx::get_lowest_epoch();
    for (auto&& oe : ti->get_overtaken_ltx_set()) {
        wp::wp_meta* wp_meta_ptr{oe.first};
        std::lock_guard<std::shared_mutex> lk{
                wp_meta_ptr->get_mtx_wp_result_set()};
        bool is_first_item_before_gc_threshold{true};
        for (auto&& wp_result_itr = wp_meta_ptr->get_wp_result_set().begin();
             wp_result_itr != wp_meta_ptr->get_wp_result_set().end();) {
            // oe.second is forwarding high priori ltxs
            auto wp_result_id =
                    wp::wp_meta::wp_result_elem_extract_id((*wp_result_itr));
            auto wp_result_epoch =
                    wp::wp_meta::wp_result_elem_extract_epoch((*wp_result_itr));
            auto wp_result_was_committed =
                    wp::wp_meta::wp_result_elem_extract_was_committed(
                            (*wp_result_itr));
            for (auto&& hid : oe.second) {
                if (wp_result_id == hid && wp_result_was_committed) {
                    // the overtaken ltx was committed.
                    // the itr show overtaken ltx
                    if (wp_result_epoch < ti->get_valid_epoch()) {
                        // try forwarding
                        // check read upper bound
                        if (ti->get_read_version_max_epoch() >
                            wp_result_epoch) {
                            // forwarding break own old read
                            return Status::ERR_VALIDATION;
                        } // forwarding not break own old read
                        // lock ongoing tx for forwarding
                        std::lock_guard<std::shared_mutex> ongo_lk{
                                ongoing_tx::get_mtx()};
                        if (Status::OK !=
                            ongoing_tx::change_epoch_without_lock(
                                    ti->get_long_tx_id(), wp_result_epoch)) {
                            LOG(ERROR) << "programming error";
                            return Status::ERR_FATAL;
                        }
                        // set own epoch
                        ti->set_valid_epoch(wp_result_epoch);
                        // change wp epoch
                        change_wp_epoch(ti, wp_result_epoch);
                        /**
                         * not need extract (collect) new forwarding info,
                         * because at first touch, this tx finished that.
                         */
                    }
                    // verify success
                    break;
                }
            }

            // not match. check gc
            if (wp_result_epoch < gc_threshold) {
                // should gc
                if (is_first_item_before_gc_threshold) {
                    // not remove
                    is_first_item_before_gc_threshold = false;
                    ++wp_result_itr;
                } else {
                    // remove
                    wp_result_itr = wp_meta_ptr->get_wp_result_set().erase(
                            wp_result_itr);
                }
            } else {
                // else. should not gc
                ++wp_result_itr;
            }
        }
    }

    // verify for write set
    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        //==========
        // about point read
        // for ltx
        point_read_by_long* rbp{};
        auto rc{wp::find_read_by(wso.second.get_storage(), rbp)};
        if (rc == Status::OK) {
            if (rbp->is_exist(ti)) { return Status::ERR_VALIDATION; }
        } else {
            LOG(ERROR) << "programming error";
            return Status::ERR_FATAL;
        }

        // for stx
        auto* rec_ptr{wso.first};
        if (ti->get_valid_epoch() <= rec_ptr->get_read_by().get_max_epoch()) {
            // this will break commited stx's read
            return Status::ERR_VALIDATION;
        }
        //==========

        //==========
        // about range read
        if (wso.second.get_op() == OP_TYPE::INSERT ||
            wso.second.get_op() == OP_TYPE::DELETE) {
            // for long
            wp::page_set_meta* psm{};
            if (Status::OK ==
                wp::find_page_set_meta(wso.second.get_storage(), psm)) {
                range_read_by_long* rrbp{psm->get_range_read_by_long_ptr()};
                std::string keyb{};
                wso.first->get_key(keyb);
                auto rb{rrbp->is_exist(this_epoch, keyb)};

                if (rb) { return Status::ERR_VALIDATION; }

                range_read_by_short* rrbs{psm->get_range_read_by_short_ptr()};
                if (ti->get_valid_epoch() <= rrbs->get_max_epoch()) {
                    return Status::ERR_VALIDATION;
                }
            } else {
                LOG(ERROR) << "programming error";
                return Status::ERR_FATAL;
            }
        }
    }

    return Status::OK;
}

Status check_wait_for_preceding_bt(session* const ti) {
    if (ongoing_tx::exist_wait_for(ti)) {
        return Status::WARN_WAITING_FOR_OTHER_TX;
    }
    return Status::OK;
}

Status verify_insert(session* const ti) {
    for (auto&& wse : ti->get_write_set().get_ref_cont_for_bt()) {
        auto&& wso = std::get<1>(wse);
        if (wso.get_op() == OP_TYPE::INSERT) {
            // verify insert
            Record* rec_ptr{wso.get_rec_ptr()};
            tid_word tid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
            /**
             * It doesn't need lock. Because tx ordered before this is nothing 
             * in this timing.
             */
            if (!(tid.get_latest() && tid.get_absent())) {
                // someone interrupt tombstone
                return Status::ERR_FAIL_INSERT;
            }
        }
    }
    return Status::OK;
}

extern Status commit(session* const ti, // NOLINT
                     [[maybe_unused]] commit_param* const cp) {
    // check premature
    if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }

    /**
     * WP2: If it is possible to prepend the order, it waits for a transaction 
     * with a higher priority than itself to finish the operation.
     */
    // check wait
    auto rc = check_wait_for_preceding_bt(ti);
    if (rc != Status::OK) {
        ti->set_tx_state_if_valid(TxState::StateKind::WAITING_CC_COMMIT);
        return Status::WARN_WAITING_FOR_OTHER_TX;
    }

    // ==========
    // verify : start
    // verify read by
    rc = verify_read_by(ti);
    if (rc == Status::ERR_VALIDATION) {
        abort(ti);
        ti->set_result(reason_code::COMMITTED_READ_PROTECTION);
        return Status::ERR_VALIDATION;
    }

    // verify insert
    rc = verify_insert(ti);
    if (rc == Status::ERR_FAIL_INSERT) {
        abort(ti);
        ti->set_result(reason_code::INSERT_EXISTING_KEY);
        return Status::ERR_FAIL_INSERT;
    }
    // verify : end
    // ==========

    // This tx must success.

    register_read_by(ti);

    expose_local_write(ti);
#if defined(PWAL)
    auto oldest_log_epoch{ti->get_lpwal_handle().get_min_log_epoch()};
    // think the wal buffer is empty due to background thread's work
    if (oldest_log_epoch != 0 && // mean the wal buffer is not empty.
        oldest_log_epoch != epoch::get_global_epoch()) {
        // should flush
        shirakami::lpwal::flush_log(static_cast<void*>(ti));
    }
#endif

    // todo enhancement
    /**
     * Sort by wp and then globalize the local write set. 
     * Eliminate wp from those that have been globalized in wp units.
     */


    // about transaction state
    // this should before clean up
    // todo fix
    ti->set_tx_state_if_valid(TxState::StateKind::DURABLE);

    // clean up
    cleanup_process(ti, true);

    // set transaction result
    ti->set_result(reason_code::UNKNOWN);

    return Status::OK;
}

// ==============================

} // namespace shirakami::long_tx