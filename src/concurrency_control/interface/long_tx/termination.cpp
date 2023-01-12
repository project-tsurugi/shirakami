
#include <algorithm>
#include <map>
#include <string_view>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

#include "concurrency_control/interface/long_tx/include/long_tx.h"

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

static inline void expose_local_write(
        session* ti, tid_word& committed_id,
        std::map<Storage, std::tuple<std::string, std::string>>& write_range) {
    tid_word ctid{};
    compute_tid(ti, ctid);
    committed_id = ctid;

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
                LOG(ERROR) << log_location_prefix << "unknown operation type.";
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
                    LOG(ERROR) << log_location_prefix << "programming error";
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
        std::string_view pkey_view = wso.second.get_rec_ptr()->get_key_view();
        auto wr_itr = write_range.find(wso.second.get_storage());
        if (wr_itr != write_range.end()) {
            // found
            // more than one write
            if (pkey_view < std::get<0>(wr_itr->second)) {
                std::get<0>(wr_itr->second) = pkey_view;
            }
            if (pkey_view > std::get<1>(wr_itr->second)) {
                std::get<1>(wr_itr->second) = pkey_view;
            }
        } else {
            // not found
            write_range.insert(
                    std::make_pair(wso.second.get_storage(),
                                   std::make_tuple(std::string(pkey_view),
                                                   std::string(pkey_view))));
        }
        process(wso, ctid);
    }
}

static inline void register_wp_result_and_remove_wps(
        session* ti, const bool was_committed,
        std::map<Storage, std::tuple<std::string, std::string>>& write_range) {
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
            LOG(ERROR) << log_location_prefix << "programing error: " << rc;
            return;
        }

        // check write range
        auto wr_itr = write_range.find(storage);
        bool write_something = (wr_itr != write_range.end());
        std::string_view write_range_left{};
        std::string_view write_range_right{};
        if (write_something) {
            write_range_left = std::get<0>(wr_itr->second);
            write_range_right = std::get<1>(wr_itr->second);
        }

        // register wp result and remove wp
        if (Status::OK !=
            (*out.first)
                    ->get_wp_meta_ptr()
                    ->register_wp_result_and_remove_wp(std::make_tuple(
                            ti->get_valid_epoch(), ti->get_long_tx_id(),
                            was_committed,
                            std::make_tuple(write_something,
                                            std::string(write_range_left),
                                            std::string(write_range_right))))) {
            LOG(ERROR);
        }
    }
}

static inline void cleanup_process(
        session* const ti, const bool was_committed,
        std::map<Storage, std::tuple<std::string, std::string>>& write_range) {
    // global effect
    register_wp_result_and_remove_wps(ti, was_committed, write_range);
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
    std::map<Storage, std::tuple<std::string, std::string>> dummy;
    cleanup_process(ti, false, dummy);
    return Status::OK;
}

void register_read_by(session* const ti) {
    // point read
    // register to page info
    for (auto&& elem : ti->read_set_for_ltx().set()) {
        elem->get_point_read_by_long().push(
                {ti->get_valid_epoch(), ti->get_long_tx_id()});
    }

    // range read
    for (auto&& elem : ti->get_range_read_by_long_set()) {
        std::get<0>(elem)->push({ti->get_valid_epoch(), ti->get_long_tx_id(),
                                 std::get<1>(elem), std::get<2>(elem),
                                 std::get<3>(elem), std::get<4>(elem)});
    }
}

Status verify(session* const ti) {
    auto this_epoch = ti->get_valid_epoch();

    // forwarding verify
    auto gc_threshold = ongoing_tx::get_lowest_epoch();
    {
        // get mutex for overtaken ltx set
        std::shared_lock<std::shared_mutex> lk{ti->get_mtx_overtaken_ltx_set()};
        for (auto&& oe : ti->get_overtaken_ltx_set()) {
            wp::wp_meta* wp_meta_ptr{oe.first};
            std::shared_lock<std::shared_mutex> lk{
                    wp_meta_ptr->get_mtx_wp_result_set()};
            bool is_first_item_before_gc_threshold{true};
            std::tuple<std::string, std::string> read_range =
                    std::get<1>(oe.second);
            for (auto&& wp_result_itr =
                         wp_meta_ptr->get_wp_result_set().begin();
                 wp_result_itr != wp_meta_ptr->get_wp_result_set().end();) {
                // oe.second is forwarding high priori ltxs
                auto wp_result_id = wp::wp_meta::wp_result_elem_extract_id(
                        (*wp_result_itr));
                auto wp_result_epoch =
                        wp::wp_meta::wp_result_elem_extract_epoch(
                                (*wp_result_itr));
                auto wp_result_was_committed =
                        wp::wp_meta::wp_result_elem_extract_was_committed(
                                (*wp_result_itr));
                auto write_result =
                        wp::wp_meta::wp_result_elem_extract_write_result(
                                (*wp_result_itr));
                if (wp_result_was_committed) {
                    /**
                      * the target ltx was commited, so it needs to check.
                      */
                    for (auto&& hid : std::get<0>(oe.second)) {
                        if (wp_result_id == hid) {
                            // check conflict
                            if (!std::get<0>(write_result)) {
                                // the ltx didn't write.
                                break;
                            }
                            if (!((std::get<0>(read_range) <=
                                           std::get<1>(write_result) &&
                                   std::get<1>(write_result) <=
                                           std::get<1>(read_range)) ||
                                  (std::get<0>(read_range) <=
                                           std::get<2>(write_result) &&
                                   std::get<2>(write_result) <=
                                           std::get<1>(read_range)))) {
                                // can't hit
                                break;
                            }

                            // the itr show overtaken ltx
                            if (wp_result_epoch < ti->get_valid_epoch()) {
                                // try forwarding
                                // check read upper bound
                                if (ti->get_read_version_max_epoch() >=
                                    wp_result_epoch) {
                                    // forwarding break own old read
                                    ti->set_result(
                                            reason_code::
                                                    CC_LTX_READ_UPPER_BOUND_VIOLATION);
                                    return Status::ERR_CC;
                                } // forwarding not break own old read
                                ti->set_valid_epoch(wp_result_epoch);
                            }
                            // verify success
                            break;
                        }
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
    }


    // verify for write set
    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        // check about kvs
        auto* rec_ptr{wso.first};
        tid_word tid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        if (wso.second.get_op() == OP_TYPE::INSERT) {
            // expect the record not existing
            if (!(tid.get_latest() && tid.get_absent())) {
                // someone interrupt tombstone
                ti->set_result(reason_code::KVS_INSERT);
                ti->get_result_info().set_key_storage_name(
                        rec_ptr->get_key_view(), wso.second.get_storage());
                return Status::ERR_CC;
            }
        } else if (wso.second.get_op() == OP_TYPE::UPDATE ||
                   wso.second.get_op() == OP_TYPE::DELETE) {
            // expect the record existing
            if (!(tid.get_latest() && !tid.get_absent())) {
                if (wso.second.get_op() == OP_TYPE::UPDATE) {
                    ti->get_result_info().set_key_storage_name(
                            rec_ptr->get_key_view(), wso.second.get_storage());
                    ti->set_result(reason_code::KVS_UPDATE);
                } else {
                    ti->set_result(reason_code::KVS_DELETE);
                    ti->get_result_info().set_key_storage_name(
                            rec_ptr->get_key_view(), wso.second.get_storage());
                }
                return Status::ERR_CC;
            }
        }

        //==========
        // about point read
        // for ltx
        point_read_by_long* rbp{};
        rbp = &wso.first->get_point_read_by_long();
        if (rbp->is_exist(ti)) {
            ti->get_result_info().set_key_storage_name(
                    rec_ptr->get_key_view(), wso.second.get_storage());
            ti->set_result(reason_code::CC_LTX_WRITE_COMMITTED_READ_PROTECTION);
            return Status::ERR_CC;
        }

        // for stx
        if (ti->get_valid_epoch() <= rec_ptr->get_read_by().get_max_epoch()) {
            // this will break commited stx's read
            ti->get_result_info().set_key_storage_name(
                    rec_ptr->get_key_view(), wso.second.get_storage());
            ti->set_result(reason_code::CC_LTX_WRITE_COMMITTED_READ_PROTECTION);
            return Status::ERR_CC;
        }
        //==========

        //==========
        // about range read
        if (wso.second.get_op() == OP_TYPE::INSERT ||
            wso.second.get_op() ==
                    OP_TYPE::UPSERT || // upsert may cause phantom
            wso.second.get_op() == OP_TYPE::DELETE) {
            wp::page_set_meta* psm{};
            if (Status::OK ==
                wp::find_page_set_meta(wso.second.get_storage(), psm)) {
                range_read_by_long* rrbp{psm->get_range_read_by_long_ptr()};
                std::string keyb{};
                wso.first->get_key(keyb);
                auto rb{rrbp->is_exist(this_epoch, keyb)};

                // for long
                if (rb) {
                    ti->get_result_info().set_key_storage_name(
                            rec_ptr->get_key_view(), wso.second.get_storage());
                    ti->set_result(reason_code::CC_LTX_PHANTOM_AVOIDANCE);
                    return Status::ERR_CC;
                }

                // for short
                range_read_by_short* rrbs{psm->get_range_read_by_short_ptr()};
                if (ti->get_valid_epoch() <= rrbs->get_max_epoch()) {
                    ti->get_result_info().set_key_storage_name(
                            rec_ptr->get_key_view(), wso.second.get_storage());
                    ti->set_result(reason_code::CC_LTX_PHANTOM_AVOIDANCE);
                    return Status::ERR_CC;
                }
            } else {
                LOG(ERROR) << log_location_prefix << "programming error";
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

Status verify_kvs_error(session* const ti) {
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
                ti->set_result(reason_code::KVS_INSERT);
                ti->get_result_info().set_key_storage_name(
                        rec_ptr->get_key_view(), wso.get_storage());
                return Status::ERR_KVS;
            }
        }
    }
    return Status::OK;
}

void process_tx_state(session* ti,
                      [[maybe_unused]] epoch::epoch_t durable_epoch) {
    if (ti->get_has_current_tx_state_handle()) {
#ifdef PWAL
        // this tx state is checked
        ti->get_current_tx_state_ptr()->set_durable_epoch(durable_epoch);
        ti->get_current_tx_state_ptr()->set_kind(
                TxState::StateKind::WAITING_DURABLE);
#else
        ti->get_current_tx_state_ptr()->set_kind(TxState::StateKind::DURABLE);
#endif
    }
}

extern Status commit(session* const ti) {
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
        if (!ti->get_requested_commit()) {
            // record requested
            ti->set_requested_commit(true);
            // register for background worker
            bg_work::bg_commit::register_tx(static_cast<void*>(ti));
        }
        return Status::WARN_WAITING_FOR_OTHER_TX;
    }

    // ==========
    // verify : start
    rc = verify(ti);
    if (rc == Status::ERR_CC) {
        abort(ti);
        return Status::ERR_CC;
    }
    // verify : end
    // ==========

    // This tx must success.

    register_read_by(ti);

    tid_word ctid{};
    /**
     * For registering write preserve result.
     * Null (string "") may be used for pkey, but the range expressed two string
     * don't know the endpoint is nothing or null key.
     * So it needs boolean.
     */
    std::map<Storage, std::tuple<std::string, std::string>> write_range;
    expose_local_write(ti, ctid, write_range);

    // sequence process
    // This must be after cc commit and before log process
    ti->commit_sequence(ctid);

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
    process_tx_state(ti, ti->get_valid_epoch());

    // clean up
    cleanup_process(ti, true, write_range);

    // set transaction result
    ti->set_result(reason_code::UNKNOWN);

    return Status::OK;
}

Status check_commit(Token const token) {
    auto* ti = static_cast<session*>(token);

    // check for requested commit.
    if (!ti->get_requested_commit()) { return Status::WARN_ILLEGAL_OPERATION; }

    auto rs = ti->get_result_requested_commit();
    if (rs == Status::WARN_WAITING_FOR_OTHER_TX) { return rs; }
    // the transaction was finished.
    // clear metadata about auto commit.
    ti->set_requested_commit(false);
    return rs;
}

// ==============================

} // namespace shirakami::long_tx