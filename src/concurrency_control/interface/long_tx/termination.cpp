
#include <algorithm>
#include <map>
#include <string_view>
#include <vector>

#include "atomic_wrapper.h"
#include "storage.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "database/include/logging.h"

#include "index/yakushima/include/interface.h"

#include "glog/logging.h"

namespace shirakami::long_tx {

// ==============================
// static inline functions for this source
static inline void cancel_flag_inserted_records(session* const ti) {
    auto process = [](std::pair<Record* const, write_set_obj>& wse) {
        auto&& wso = std::get<1>(wse);
        if (wso.get_op() == OP_TYPE::INSERT ||
            wso.get_op() == OP_TYPE::UPSERT) {
            auto* rec_ptr = std::get<0>(wse);

            // about tombstone count
            if (wso.get_inc_tombstone()) {
                rec_ptr->get_tidw_ref().lock();
                if (rec_ptr->get_shared_tombstone_count() == 0) {
                    LOG_FIRST_N(ERROR, 1)
                            << log_location_prefix << "unreachable path.";
                } else {
                    --rec_ptr->get_shared_tombstone_count();
                }
                rec_ptr->get_tidw_ref().unlock();
            }

            // consider sharing tombstone
            if (rec_ptr->get_shared_tombstone_count() > 0) { return; }

            tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
            // pre-check
            auto check_cd = [&check, rec_ptr]() {
                return check.get_latest() && check.get_absent() &&
                       rec_ptr->get_shared_tombstone_count() == 0;
            };
            if (check_cd()) {
                rec_ptr->get_tidw_ref().lock();
                check = loadAcquire(
                        rec_ptr->get_tidw_ref().get_obj()); // reload
                if (check_cd()) {                           // main-check
                    tid_word tid{};
                    tid.set_absent(true);
                    tid.set_latest(false);
                    tid.set_lock(false);
                    tid.set_epoch(check.get_epoch());
                    // XXX: need restore tid (minor write vesion)??
                    rec_ptr->set_tid(tid); // and unlock
                } else {
                    rec_ptr->get_tidw_ref().unlock();
                }
            }
        }
    };

    std::shared_lock<std::shared_mutex> lk{ti->get_write_set().get_mtx()};
    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        process(wso);
    }
}

static inline void compute_tid(session* ti, tid_word& ctid) {
    ctid.set_epoch(ti->get_valid_epoch());
    ctid.set_tid(ti->get_long_tx_id());
    ctid.set_lock(false);
    ctid.set_absent(false);
    ctid.set_latest(true);
    ctid.set_by_short(false);
}

static inline void expose_local_write(
        session* ti, tid_word& committed_id,
        std::map<Storage, std::tuple<std::string, std::string>>& write_range) {
    tid_word ctid{};
    compute_tid(ti, ctid);
    committed_id = ctid;

    //bool should_backward{ti->is_write_only_ltx_now()};
    bool should_backward{!ti->get_is_forwarding()};
    auto process = [ti, should_backward](
                           std::pair<Record* const, write_set_obj>& wse,
                           tid_word ctid) {
        auto* rec_ptr = std::get<0>(wse);
        auto&& wso = std::get<1>(wse);
        [[maybe_unused]] bool should_log{true};
        // bw can backward including occ bw

        // about tombstone count
        if (wso.get_inc_tombstone()) {
            auto* rec_ptr = wso.get_rec_ptr();
            rec_ptr->get_tidw_ref().lock();
            if (rec_ptr->get_shared_tombstone_count() == 0) {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "unreachable path.";
            } else {
                --rec_ptr->get_shared_tombstone_count();
            }
            rec_ptr->get_tidw_ref().unlock();
        }
        switch (wso.get_op()) {
            case OP_TYPE::UPSERT:
            case OP_TYPE::INSERT: {
                tid_word tid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
                auto check_cd = [&tid]() {
                    return tid.get_absent() && // inserting or deleted
                           // DELETE'd Record (not-absent -> deleted) has non-zero epoch/tid
                           tid.get_epoch() == 0 && tid.get_tid() == 0;
                };
                if (check_cd()) {
                    // lock record
                    rec_ptr->get_tidw_ref().lock();
                    tid = loadAcquire(
                            rec_ptr->get_tidw_ref().get_obj()); // reload
                    if (check_cd()) {                           // re-check
                        // update value
                        std::string vb{};
                        wso.get_value(vb);
                        rec_ptr->get_latest()->set_value(vb);
                        // unlock and set ctid
                        rec_ptr->set_tid(ctid);
                        break;
                    }
                    rec_ptr->get_tidw_ref().unlock();
                }
                [[fallthrough]]; // upsert is update
            }
            case OP_TYPE::DELETE:
            case OP_TYPE::DELSERT:
            case OP_TYPE::TOMBSTONE: {
                if (wso.get_op().is_wso_to_absent()) { // for fallthrough
                    if (rec_ptr->get_shared_tombstone_count() == 0) {
                        ctid.set_latest(false);
                        ctid.set_absent(true);
                    } else { // consider for sharing tombstone by insert, block gc
                        ctid.set_latest(true);
                        ctid.set_absent(true);
                    }
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
                    if (wso.get_op().is_wso_to_alive()) { wso.get_value(vb); }
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
                    if (should_backward && !pre_tid.get_by_short() &&
                        ti->get_long_tx_id() > pre_tid.get_tid()) {
                        /**
                         * non invisible write due to
                         * 1: write only
                         * 2: no waiting bypass and no forwarding
                         */
                        should_log = true;
                        std::string vb{};
                        wso.get_value(vb);
                        rec_ptr->get_latest()->set_value(vb);
                    } else {
                        // invisible write
                        should_log = false;
                        // keep tx id and by_short bit for successor invisible write

                        ctid.set_tid(pre_tid.get_tid());
                        ctid.set_by_short(pre_tid.get_by_short());
                        // keep entry status (existing, deleted)
                        ctid.set_latest(pre_tid.get_latest());
                        ctid.set_absent(pre_tid.get_absent());
                    }
                    // unlock and set ctid
                    rec_ptr->set_tid(ctid);
                } else {
                    // case: middle of list
                    auto version_creation = [&wso, ctid](version* pre_ver,
                                                         version* ver) {
                        std::string vb{};
                        if (wso.get_op().is_wso_to_alive()) {
                            // load payload if not delete.
                            wso.get_value(vb);
                        }
                        version* new_v{new version(ctid, vb, ver)}; // NOLINT
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
                            // para (partial order) write, normally invisible write
                            if (should_backward && !tid.get_by_short() &&
                                ti->get_long_tx_id() > tid.get_tid()) {
                                // non invisible write due to bypass read wait
                                std::string vb{};
                                wso.get_value(vb);
                                // set value
                                ver->set_value(vb);
                                ver->set_tid(ctid);
                            }
                            // else: omit due to forwarding
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
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "unknown operation type.";
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
                case OP_TYPE::DELETE:
                case OP_TYPE::DELSERT:
                case OP_TYPE::TOMBSTONE: {
                    lo = log_operation::DELETE;
                    break;
                }
                default: {
                    LOG_FIRST_N(ERROR, 1)
                            << log_location_prefix << "unknown operation type.";
                    return Status::ERR_FATAL;
                }
            }
            ti->get_lpwal_handle().push_log(shirakami::lpwal::log_record(
                    lo,
                    lpwal::write_version_type(ti->get_valid_epoch(),
                                              ti->get_long_tx_id()),
                    wso.get_storage(), key, val, wso.get_lobs()));
        }
#endif
        return Status::OK;
    };

#ifdef PWAL
    std::unique_lock<std::mutex> lk{ti->get_lpwal_handle().get_mtx_logs()};
#endif
    {
        std::shared_lock<std::shared_mutex> lk{ti->get_write_set().get_mtx()};
        for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
            std::string_view pkey_view =
                    wso.second.get_rec_ptr()->get_key_view();
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
                write_range.insert(std::make_pair(
                        wso.second.get_storage(),
                        std::make_tuple(std::string(pkey_view),
                                        std::string(pkey_view))));
            }
            process(wso, ctid);
        }
    }
}

static inline void register_wp_result_and_remove_wps(
        session* ti, const bool was_committed,
        std::map<Storage, std::tuple<std::string, std::string>>& write_range) {
    for (auto&& elem : ti->get_wp_set()) {
        Storage storage = elem.first;
        // check the storage is valid yet
        wp::page_set_meta* target_psm_ptr{};
        auto ret = wp::find_page_set_meta(storage, target_psm_ptr);
        if (ret != Status::OK ||
            // check the ptr was not changed
            (ret == Status::OK &&
             elem.second != target_psm_ptr->get_wp_meta_ptr())) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix
                                  << "Error. Suspected mix of DML and DDL";
            continue;
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
            (elem.second->register_wp_result_and_remove_wp(std::make_tuple(
                    ti->get_valid_epoch(), ti->get_long_tx_id(), was_committed,
                    std::make_tuple(write_something,
                                    std::string(write_range_left),
                                    std::string(write_range_right)))))) {
            LOG_FIRST_N(ERROR, 1)
                    << "Fail to register wp result and remove wp.";
        }
    }
}

static inline void cleanup_process(
        session* const ti, const bool was_committed,
        std::map<Storage, std::tuple<std::string, std::string>>& write_range) {
    // global effect
    register_wp_result_and_remove_wps(ti, was_committed, write_range);
    ongoing_tx::remove_id(ti->get_long_tx_id());

    // clear about read plan
    read_plan::remove_elem(ti->get_long_tx_id());

    // local effect
    ti->clean_up();
}

// ==============================

// ==============================
// functions declared at header
Status abort(session* const ti) { // NOLINT
    // about tx state
    ti->set_tx_state_if_valid(TxState::StateKind::ABORTED);

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
    {
        // take read lock
        std::shared_lock<std::shared_mutex> lk{
                ti->read_set_for_ltx().get_mtx_set()};
        for (auto&& elem : ti->read_set_for_ltx().set()) {
            elem->get_point_read_by_long().push(
                    {ti->get_valid_epoch(), ti->get_long_tx_id()});
        }
    }

    // range read
    {
        // take read lock
        std::shared_lock<std::shared_mutex> lk{
                ti->get_range_read_set_for_ltx().get_mtx_set()};
        for (auto&& elem : ti->get_range_read_set_for_ltx().get_set()) {
            std::get<0>(elem)->push({ti->get_valid_epoch(),
                                     ti->get_long_tx_id(), std::get<1>(elem),
                                     std::get<2>(elem), std::get<3>(elem),
                                     std::get<4>(elem)});
        }
    }
}

Status verify(session* const ti) {
    // forwarding verify
    auto gc_threshold = epoch::get_cc_safe_ss_epoch();
    {
        // get mutex for overtaken ltx set
        std::shared_lock<std::shared_mutex> lk{ti->get_mtx_overtaken_ltx_set()};
        for (auto&& oe : ti->get_overtaken_ltx_set()) {
            wp::wp_meta* wp_meta_ptr{oe.first};
            std::lock_guard<std::shared_mutex> lk{
                    wp_meta_ptr->get_mtx_wp_result_set()};
            bool is_first_item_before_gc_threshold{true};
            auto read_range = std::get<1>(oe.second);
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
                            // def ramda for hit
                            auto hit_process = [ti, wp_result_epoch]() {
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
                                return Status::OK;
                            };
                            if (
                                    /**
                                     * read right point < write left point
                                     */
                                    (std::get<2>(read_range) <
                                             std::get<1>(write_result) &&
                                     std::get<3>(read_range) !=
                                             scan_endpoint::INF) ||
                                    /**
                                     * write right point < read left point
                                     */
                                    (std::get<2>(write_result) <
                                             std::get<0>(read_range) &&
                                     std::get<1>(read_range) !=
                                             scan_endpoint::INF)) {
                                // can't hit
                                break;
                            }

                            // hit process, forwarding
                            // register whether forwarding
                            ti->set_is_forwarding(true);
                            if (wp_result_epoch < ti->get_valid_epoch()) {
                                // need to update epoch
                                auto rc = hit_process();
                                if (rc == Status::ERR_CC) { return rc; }
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
            /**
             * wp result set から見つからないということは、相手は wp 宣言をしたが
             * 実際には書かなくて wp が縮退されたということ。そのままスルーしてよい。
             * イテレートは拡張 for ループの方で実施される。
             */
        }
    }
    if (ti->get_is_forwarding()) {
        // verify for write set
        {
            std::shared_lock<std::shared_mutex> lk{
                    ti->get_write_set().get_mtx()};
            for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
                // check about kvs
                auto* rec_ptr{wso.first};
                tid_word tid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
                if (wso.second.get_op().is_wso_from_absent()) {
                    // expect the record not existing
                    if (!(tid.get_latest() && tid.get_absent())) {
                        // someone interrupt tombstone
                        std::unique_lock<std::mutex> lk{
                                ti->get_mtx_result_info()};
                        ti->set_result(reason_code::KVS_INSERT);
                        ti->get_result_info().set_key_storage_name(
                                rec_ptr->get_key_view(),
                                wso.second.get_storage());
                        return Status::ERR_CC;
                    }
                } else if (wso.second.get_op() == OP_TYPE::UPDATE ||
                           wso.second.get_op() == OP_TYPE::DELETE) {
                    // expect the record existing
                    if (!(tid.get_latest() && !tid.get_absent())) {
                        // XXX: OP might be combined, so the type of first op can loss
                        if (wso.second.get_op() == OP_TYPE::UPDATE) {
                            ti->set_result(reason_code::KVS_UPDATE);
                        } else {
                            ti->set_result(reason_code::KVS_DELETE);
                        }
                        std::unique_lock<std::mutex> lk{
                                ti->get_mtx_result_info()};
                        ti->get_result_info().set_key_storage_name(
                                rec_ptr->get_key_view(),
                                wso.second.get_storage());
                        return Status::ERR_CC;
                    }
                }

                //==========
                // about point read
                // for ltx
                point_read_by_long* rbp{};
                rbp = &wso.first->get_point_read_by_long();
                if (rbp->is_exist(ti)) {
                    std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
                    ti->get_result_info().set_key_storage_name(
                            rec_ptr->get_key_view(), wso.second.get_storage());
                    ti->set_result(
                            reason_code::
                                    CC_LTX_WRITE_COMMITTED_READ_PROTECTION);
                    return Status::ERR_CC;
                }

                // for stx
                if (ti->get_valid_epoch() <=
                    rec_ptr->get_read_by().get_max_epoch()) {
                    // this will break commited stx's read
                    std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
                    ti->get_result_info().set_key_storage_name(
                            rec_ptr->get_key_view(), wso.second.get_storage());
                    ti->set_result(
                            reason_code::
                                    CC_LTX_WRITE_COMMITTED_READ_PROTECTION);
                    return Status::ERR_CC;
                }
                //==========

                //==========
                // about range read
                if (wso.second.get_op() == OP_TYPE::INSERT ||
                    wso.second.get_op() ==
                            OP_TYPE::UPSERT || // upsert may cause phantom
                    wso.second.get_op() == OP_TYPE::DELETE) {
                    // XXX: why DELETE? this is wso to absent, so cause no phantom
                    wp::page_set_meta* psm{};
                    if (Status::OK ==
                        wp::find_page_set_meta(wso.second.get_storage(), psm)) {
                        range_read_by_long* rrbp{
                                psm->get_range_read_by_long_ptr()};
                        std::string keyb{};
                        wso.first->get_key(keyb);
                        auto rb{rrbp->is_exist(ti->get_valid_epoch(),
                                               ti->get_long_tx_id(), keyb)};

                        // for long
                        if (rb) {
                            std::unique_lock<std::mutex> lk{
                                    ti->get_mtx_result_info()};
                            ti->get_result_info().set_key_storage_name(
                                    rec_ptr->get_key_view(),
                                    wso.second.get_storage());
                            ti->set_result(
                                    reason_code::CC_LTX_PHANTOM_AVOIDANCE);
                            return Status::ERR_CC;
                        }

                        // for short
                        range_read_by_short* rrbs{
                                psm->get_range_read_by_short_ptr()};
                        if (ti->get_valid_epoch() <= rrbs->get_max_epoch()) {
                            std::unique_lock<std::mutex> lk{
                                    ti->get_mtx_result_info()};
                            ti->get_result_info().set_key_storage_name(
                                    rec_ptr->get_key_view(),
                                    wso.second.get_storage());
                            ti->set_result(
                                    reason_code::CC_LTX_PHANTOM_AVOIDANCE);
                            return Status::ERR_CC;
                        }
                    } else {
                        LOG_FIRST_N(ERROR, 1)
                                << log_location_prefix
                                << "Fail to find wp page set meta.";
                        return Status::ERR_FATAL;
                    }
                }
            }
        }
    }

    return Status::OK;
}

Status check_wait_for_preceding_bt(session* const ti) {
    Status rc{};
    if (ongoing_tx::exist_wait_for(ti, rc)) {
        return Status::WARN_WAITING_FOR_OTHER_TX;
    }
    return rc;
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

void update_read_area(session* const ti) {
    if (ti->get_ltx_storage_read_set().empty()) {
        // write only tx
        read_plan::add_elem(ti->get_long_tx_id(),
                            {{storage::dummy_storage}, {}});
        return;
    }

    // update
    read_plan::plist_type plist;
    read_plan::nlist_type nlist;
    for (auto&& elem : ti->get_ltx_storage_read_set()) {
        plist.insert(std::make_tuple(elem.first, true, std::get<0>(elem.second),
                                     std::get<1>(elem.second),
                                     std::get<2>(elem.second),
                                     std::get<3>(elem.second)));
    }
    read_plan::add_elem(ti->get_long_tx_id(), plist, nlist);
}

void call_commit_callback(commit_callback_type const& cb, Status sc,
                          reason_code rc, durability_marker_type dm) {
    if (cb) { cb(sc, rc, dm); }
}

extern Status commit(session* const ti) {
    // check premature
    if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
        ti->call_commit_callback(Status::WARN_PREMATURE, {}, 0);
        return Status::WARN_PREMATURE;
    }

    // update read area
    update_read_area(ti);

    // detail info
    if (logging::get_enable_logging_detail_info()) {
        // extract wait for
        auto wait_for = ti->extract_wait_for();
        std::string wait_for_str{};
        for (auto elem : wait_for) {
            wait_for_str.append(std::to_string(elem) + ", ");
        }
        VLOG(log_trace) << log_location_prefix_detail_info
                        << "commit request accept, LTX, tx id: "
                        << ti->get_long_tx_id()
                        << ", wait for ltx id: " << wait_for_str;
    }
    // log debug timing event
    std::string str_tx_id{};
    get_tx_id(static_cast<Token>(ti), str_tx_id);
    VLOG(log_debug) << log_location_prefix << str_tx_id << ": "
                    << "ltx start to check wait";

    /**
     * WP2: If it is possible to prepend the order, it waits for a transaction
     * with a higher priority than itself to finish the operation.
     */
    // check wait
    auto rc = check_wait_for_preceding_bt(ti);
    if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
        ti->set_tx_state_if_valid(TxState::StateKind::WAITING_CC_COMMIT);
        if (!ti->get_requested_commit()) {
            // start wait
            // log debug timing event
            VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                         << "start_wait : " << str_tx_id;

            // record requested
            ti->set_requested_commit(true);
            // register for background worker
            bg_work::bg_commit::register_tx(static_cast<void*>(ti));
        }
        return Status::WARN_WAITING_FOR_OTHER_TX;
    }
    if (rc == Status::ERR_CC) {
        // log debug timing event
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "start_abort : " << str_tx_id;
        long_tx::abort(ti);
        ti->call_commit_callback(rc, ti->get_result_info().get_reason_code(),
                                 0);
        goto END_COMMIT; // NOLINT
    }
    if (rc != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix << "library programming error. " << rc;
        return rc;
    }

    // log debug timing event
    VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                 << "start_verify : " << str_tx_id;

    // verify not write only : start
    if (!ti->get_ltx_storage_read_set().empty()) {
        rc = verify(ti);
    } else {
        rc = Status::OK;
    }

    if (rc == Status::ERR_CC) {
        // verfy fail
        // log debug timing event
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "start_abort : " << str_tx_id;
        long_tx::abort(ti);
        // log debug timing event
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "end_abort : " << str_tx_id;
        ti->call_commit_callback(rc, ti->get_result_info().get_reason_code(),
                                 0);
    } else if (rc == Status::OK) {
        // This tx must success.

        // log debug timing event
        if (ti->get_requested_commit()) {
            VLOG(log_debug_timing_event)
                    << log_location_prefix_timing_event
                    << "precommit_with_wait : " << str_tx_id;
        } else {
            VLOG(log_debug_timing_event)
                    << log_location_prefix_timing_event
                    << "precommit_with_nowait : " << str_tx_id;
        }

        VLOG(log_debug_timing_event)
                << log_location_prefix_timing_event
                << "start_register_read_by : " << str_tx_id;

        register_read_by(ti);

        // log debug timing event
        VLOG(log_debug_timing_event)
                << log_location_prefix_timing_event
                << "start_expose_local_write : " << str_tx_id;

        tid_word ctid{};
        /**
         * For registering write preserve result.
         * Null (string "") may be used for pkey, but the range expressed two string
         * don't know the endpoint is nothing or null key.
         * So it needs boolean.
         */
        std::map<Storage, std::tuple<std::string, std::string>> write_range;
        expose_local_write(ti, ctid, write_range);

        // log debug timing event
        VLOG(log_debug_timing_event)
                << log_location_prefix_timing_event
                << "start_process_sequence : " << str_tx_id;

        // sequence process
        // This must be after cc commit and before log process
        ti->commit_sequence(ctid);

        // todo enhancement
        /**
         * Sort by wp and then globalize the local write set.
         * Eliminate wp from those that have been globalized in wp units.
         */

        auto this_dm = epoch::get_global_epoch();
#if defined(PWAL)
        {
            auto& handle = ti->get_lpwal_handle();
            std::unique_lock lk{handle.get_mtx_logs()};
            if (handle.get_begun_session()) { this_dm = handle.get_durable_epoch(); }
        }
#endif

        // about transaction state
        process_tx_state(ti, this_dm);

        // clean up
        cleanup_process(ti, true, write_range);

        // set transaction result
        ti->set_result(reason_code::UNKNOWN);

        // call commit callback
        ti->call_commit_callback(rc, {}, this_dm);

        // log debug timing event
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "start_process_logging : " << str_tx_id;

#if defined(PWAL)
        auto oldest_log_epoch{ti->get_lpwal_handle().get_min_log_epoch()};
        // think the wal buffer is empty due to background thread's work
        if (oldest_log_epoch != 0 && // mean the wal buffer is not empty.
            oldest_log_epoch != epoch::get_global_epoch()) {
            // should flush
            shirakami::lpwal::flush_log(static_cast<void*>(ti));
        }
#endif

    } else {
        LOG_FIRST_N(ERROR, 1) << "library programming error.";
    }

END_COMMIT: // NOLINT
    // log debug timing event
    VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                 << "end_precommit : " << str_tx_id;

    // detail info
    if (logging::get_enable_logging_detail_info()) {
        VLOG(log_trace) << log_location_prefix_detail_info
                        << "commit request processed, LTX, tx id: "
                        << ti->get_long_tx_id() << ", return code: " << rc;
    }
    return rc;
}

Status check_commit(Token const token) { // NOLINT
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
