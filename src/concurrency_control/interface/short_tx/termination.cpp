
#include "atomic_wrapper.h"
#include "storage.h"

#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "database/include/logging.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/logging.h"

#include "glog/logging.h"

namespace shirakami::short_tx {

// ==========
// locking
void unlock_write_set(session* const ti) {
    auto process = [](Record* rec_ptr) {
        // detail info
        if (logging::get_enable_logging_detail_info()) {
            VLOG(log_trace)
                    << log_location_prefix_detail_info
                    << "unlock key " + std::string(rec_ptr->get_key_view());
        }
        rec_ptr->get_tidw_ref().unlock();
    };
    {
        std::shared_lock<std::shared_mutex> lk{ti->get_write_set().get_mtx()};
        if (ti->get_write_set().get_for_batch()) {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_bt()) {
                process(itr.first);
            }
        } else {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
                process(itr.get_rec_ptr());
            }
        }
    }
}


void unlock_records(session* const ti, std::size_t num_locked) {
    auto process = [&num_locked](write_set_obj* wso_ptr) {
        if (num_locked == 0) { return; }
        // detail info
        if (logging::get_enable_logging_detail_info()) {
            VLOG(log_trace)
                    << log_location_prefix_detail_info
                    << "unlock key " +
                               std::string(
                                       wso_ptr->get_rec_ptr()->get_key_view());
        }

        wso_ptr->get_rec_ptr()->get_tidw_ref().unlock();

        // detail info
        if (logging::get_enable_logging_detail_info()) {
            VLOG(log_trace)
                    << log_location_prefix_detail_info
                    << "unlocked key " +
                               std::string(
                                       wso_ptr->get_rec_ptr()->get_key_view());
        }

        --num_locked;
    };
    {
        std::shared_lock<std::shared_mutex> lk{ti->get_write_set().get_mtx()};
        if (ti->get_write_set().get_for_batch()) {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_bt()) {
                process(&itr.second);
                if (num_locked == 0) { break; }
            }
        } else {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
                process(&itr);
                if (num_locked == 0) { break; }
            }
        }
    }
}

/**
 * This is called by only abort function
*/
void change_inserting_records_state(session* const ti) {
    auto process = [](write_set_obj* wso_ptr) {
        Record* rec_ptr = wso_ptr->get_rec_ptr();
        if (wso_ptr->get_op() == OP_TYPE::INSERT ||
            wso_ptr->get_op() == OP_TYPE::UPSERT) {
            // about tombstone count
            if (wso_ptr->get_inc_tombstone()) { // did inc
                rec_ptr->get_tidw_ref().lock();
                if (rec_ptr->get_shared_tombstone_count() == 0) {
                    LOG_FIRST_N(ERROR, 1)
                            << log_location_prefix << "unreachable path.";
                } else {
                    --rec_ptr->get_shared_tombstone_count();
                }
                rec_ptr->get_tidw_ref().unlock();
            }

            tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
            // pre-check
            auto check_cd = [&check]() {
                return check.get_latest() && check.get_absent();
            };
            if (check_cd()) { // inserting state
                rec_ptr->get_tidw_ref().lock();
                check = loadAcquire(rec_ptr->get_tidw_ref().get_obj());

                // detail info
                if (logging::get_enable_logging_detail_info()) {
                    VLOG(log_trace)
                            << log_location_prefix_detail_info
                            << "unlock key " +
                                       std::string(rec_ptr->get_key_view());
                }

                // consider sharing tombstone
                if (rec_ptr->get_shared_tombstone_count() > 0) {
                    rec_ptr->get_tidw_ref().unlock();
                    return;
                }

                // main-check
                if (check_cd()) {
                    // inserting yet
                    tid_word tid{};
                    tid.set_absent(true);
                    tid.set_latest(false);
                    tid.set_lock(false);
                    tid.set_epoch(check.get_epoch());
                    tid.set_tid(check.get_tid());
                    rec_ptr->set_tid(tid); // and unlock
                } else {
                    // some operations interrupt and this is normal state.
                    rec_ptr->get_tidw_ref().unlock();
                }
            }
        }
    };
    {
        std::shared_lock<std::shared_mutex> lk{ti->get_write_set().get_mtx()};
        if (ti->get_write_set().get_for_batch()) {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_bt()) {
                process(&itr.second);
            }
        } else {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
                process(&itr);
            }
        }
    }
}
// ==========

Status abort(session* ti) { // NOLINT
    // about tx state
    ti->set_tx_state_if_valid(TxState::StateKind::ABORTED);

    /**
     * About inserted records.
     * This is for read phase user abort.
     * If this is not, the inserting record's inserting state will be not changed.
     */
    change_inserting_records_state(ti);

    ti->clean_up();
    return Status::OK;
}

Status read_verify(session* ti, tid_word read_tid, tid_word check,
                   const Record* const rec_ptr) {
    if (
            // different tid
            read_tid.get_tid() != check.get_tid() ||
            // different epoch
            read_tid.get_epoch() != check.get_epoch() ||
            // locked and it's not own.
            (check.get_lock() &&
             ti->get_write_set().search(rec_ptr) == nullptr)) {
        return Status::ERR_CC;
    }
    return Status::OK;
}

Status wp_verify(Storage const st, epoch::epoch_t const commit_epoch) {
    wp::wp_meta* wm{};
    auto rc{find_wp_meta(st, wm)};
    if (rc != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "unreachable path. It strongly suspect that DML and DDL "
                   "are mixed.";
    }
    auto wps{wm->get_wped()};
    auto find_min_ep{wp::wp_meta::find_min_ep(wps)};
    if (find_min_ep != 0 && find_min_ep <= commit_epoch) {
        return Status::ERR_CC;
    }
    return Status::OK;
}

Status read_wp_verify(session* const ti, epoch::epoch_t ce,
                      tid_word& commit_tid) {
    tid_word check{};
    std::vector<Storage> accessed_st{};
    // read verify
    for (auto&& itr : ti->get_read_set_for_stx()) {
        auto* rec_ptr = itr.get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw_ref().get_obj());

        while (check.get_lock_by_gc()) {
            // gc takes locks equal or less than one lock.
            _mm_pause();
            check.get_obj() = loadAcquire(rec_ptr->get_tidw_ref().get_obj());
        }

        // verify
        // ==============================
        if (read_verify(ti, itr.get_tid(), check, rec_ptr) != Status::OK) {
            unlock_write_set(ti);
            ti->get_result_info().set_key_storage_name(rec_ptr->get_key_view(),
                                                       itr.get_storage());
            ti->set_result(reason_code::CC_OCC_READ_VERIFY);
            short_tx::abort(ti);
            return Status::ERR_CC;
        }
        // ==============================

        // log accessed storage
        accessed_st.emplace_back(itr.get_storage());

        // compute timestamp
        commit_tid = std::max(check, commit_tid);
    }

    // wp verify
    // reduce redundat
    std::sort(accessed_st.begin(), accessed_st.end());
    accessed_st.erase(std::unique(accessed_st.begin(), accessed_st.end()),
                      accessed_st.end());
    for (auto&& each_st : accessed_st) {
        if (wp_verify(each_st, ce) != Status::OK) {
            unlock_write_set(ti);
            short_tx::abort(ti);
            ti->set_result(reason_code::CC_OCC_WP_VERIFY);
            return Status::ERR_CC;
        }
    }

    return Status::OK;
}

Status sert_process_at_write_lock(write_set_obj* wso) {
    // check key exists yet
    std::string key{};
    wso->get_rec_ptr()->get_key(key);
    Record* rec_ptr{};
    auto rc = get<Record>(wso->get_storage(), key, rec_ptr);
    if (rc == Status::OK) {
        // the key exists and is hooked.
        // point (*1)
        if (wso->get_rec_ptr() != rec_ptr) {
            /**
             * if insert, it already incremented shared tombstone count.
             * if upsert, it cant be unhooked.
            */
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            wso->set_rec_ptr(rec_ptr); // for fail safe
        }

        // check ts
        tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};

        // detail info
        if (logging::get_enable_logging_detail_info()) {
            VLOG(log_trace)
                    << log_location_prefix_detail_info
                    << "lock key " + std::string(rec_ptr->get_key_view());
        }

    RE_LOCK: // NOLINT
        // locking
        rec_ptr->get_tidw_ref().lock();

        /**
         * recheck hooked yet. maybe unhooked between checking hooking and lock
        */
        rc = get<Record>(wso->get_storage(), key, rec_ptr);
        if (rc == Status::OK) {
            if (wso->get_rec_ptr() != rec_ptr) {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "unreachable path";
                // for fail safe
                wso->get_rec_ptr()->unlock();
                wso->set_rec_ptr(rec_ptr);
                goto RE_LOCK; // NOLINT
            }
        } else {
            // already unhooked
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return Status::ERR_CC; // for fail safe
        }

        // locked and it is hooked yet

        // detail info
        if (logging::get_enable_logging_detail_info()) {
            VLOG(log_trace)
                    << log_location_prefix_detail_info
                    << "locked key " + std::string(rec_ptr->get_key_view());
        }

        check = loadAcquire(rec_ptr->get_tidw_ref().get_obj());
        if ((check.get_latest() && check.get_absent()) || // inserting state
            (!check.get_absent())                         // normal state
        ) {
            return Status::OK;
        }
        // it is deleted state
        // change it to inserting state.
        check.set_latest(true);
        rec_ptr->set_tid(check);

        return Status::OK;
    }
    LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
    return Status::ERR_CC; // for fail safe
}

Status write_lock(session* ti, tid_word& commit_tid) {
    std::size_t num_locked{0};
    // sorf if occ for deadlock avoidance
    ti->get_write_set().sort_if_ol();

    auto process = [ti, &commit_tid, &num_locked](write_set_obj* wso_ptr) {
        auto* rec_ptr{wso_ptr->get_rec_ptr()};
        auto abort_process = [ti, &num_locked]() {
            if (num_locked > 0) { unlock_records(ti, num_locked); }
            short_tx::abort(ti);
        };
        if (wso_ptr->get_op() == OP_TYPE::INSERT ||
            wso_ptr->get_op() == OP_TYPE::UPSERT) {
            // about sert common process
            auto rc = sert_process_at_write_lock(wso_ptr);
            ++num_locked;
            /**
             * NOTE: sert_process_at_write_lock must have locked the record
            */
            if (rc == Status::OK) {
                // may change op type, so should do continue explicitly.

                // about insert process
                if (wso_ptr->get_op() == OP_TYPE::INSERT) {
                    tid_word tid{rec_ptr->get_tidw_ref()};
                    if (tid.get_latest() && !tid.get_absent()) {
                        // the record is existing record (not inserting, deleted)
                        abort_process();
                        ti->get_result_info().set_reason_code(
                                reason_code::KVS_INSERT);
                        ti->get_result_info().set_key_storage_name(
                                rec_ptr->get_key_view(),
                                wso_ptr->get_storage());
                        return Status::ERR_KVS;
                    }
                }
                return Status::OK;
            }
            if (rc == Status::ERR_CC) {
                abort_process();
                return Status::ERR_CC;
            }
        } else if (wso_ptr->get_op() == OP_TYPE::UPDATE ||
                   wso_ptr->get_op() == OP_TYPE::DELETE) {
            // detail info
            if (logging::get_enable_logging_detail_info()) {
                VLOG(log_trace)
                        << log_location_prefix_detail_info
                        << "lock key " + std::string(rec_ptr->get_key_view());
            }

            // lock the record
            rec_ptr->get_tidw_ref().lock();

            // detail info
            if (logging::get_enable_logging_detail_info()) {
                VLOG(log_trace)
                        << log_location_prefix_detail_info
                        << "locked key " + std::string(rec_ptr->get_key_view());
            }

            commit_tid = std::max(commit_tid, rec_ptr->get_tidw_ref());
            ++num_locked;
            if (rec_ptr->get_tidw_ref().get_absent()) {
                abort_process();
                if (wso_ptr->get_op() == OP_TYPE::UPDATE) {
                    ti->set_result(reason_code::KVS_UPDATE);
                } else if (wso_ptr->get_op() == OP_TYPE::DELETE) {
                    ti->set_result(reason_code::KVS_DELETE);
                }
                ti->get_result_info().set_key_storage_name(
                        rec_ptr->get_key_view(), wso_ptr->get_storage());
                return Status::ERR_KVS;
            }
        } else {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return Status::ERR_FATAL;
        }

        return Status::OK;
    };
    {
        std::shared_lock<std::shared_mutex> lk{ti->get_write_set().get_mtx()};
        if (ti->get_write_set().get_for_batch()) {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_bt()) {
                auto rc = process(&itr.second);
                if (rc != Status::OK) { return rc; }
            }
        } else {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
                auto rc = process(&itr);
                if (rc != Status::OK) { return rc; }
            }
        }
    }

    return Status::OK;
}

Status write_phase(session* ti, epoch::epoch_t ce) {
    auto process = [ti, ce](write_set_obj* wso_ptr) {
        tid_word update_tid{ti->get_mrc_tid()};
        VLOG(log_trace) << "write. op type: " << wso_ptr->get_op() << ", key: "
                        << shirakami_binstring(
                                   wso_ptr->get_rec_ptr()->get_key_view())
                        << ", value: "
                        << shirakami_binstring(wso_ptr->get_value_view());
        switch (wso_ptr->get_op()) {
            case OP_TYPE::UPSERT:
            case OP_TYPE::INSERT: {
                // about tombstone count
                if (wso_ptr->get_inc_tombstone()) {
                    auto* rec_ptr = wso_ptr->get_rec_ptr();
                    // already locked tid
                    if (rec_ptr->get_shared_tombstone_count() == 0) {
                        LOG_FIRST_N(ERROR, 1)
                                << log_location_prefix << "unreachable path.";
                    } else {
                        --rec_ptr->get_shared_tombstone_count();
                    }
                }

                tid_word old_tid{wso_ptr->get_rec_ptr()->get_tidw_ref()};
                if (old_tid.get_latest() && old_tid.get_absent()) {
                    // set value
                    std::string vb{};
                    wso_ptr->get_value(vb);
                    wso_ptr->get_rec_ptr()->set_value(vb);

                    // set timestamp and unlock
                    wso_ptr->get_rec_ptr()->set_tid(update_tid);
                    break;
                }
                [[fallthrough]];
                // upsert is update
            }
            case OP_TYPE::DELETE: {
                if (wso_ptr->get_op() == OP_TYPE::DELETE) {
                    if (wso_ptr->get_rec_ptr()->get_shared_tombstone_count() ==
                        0) {
                        update_tid.set_absent(true);
                        update_tid.set_latest(false);
                    } else {
                        // consider for sharing tombstone by insert, block gc
                        update_tid.set_absent(true);
                        update_tid.set_latest(true);
                    }
                }
                [[fallthrough]];
            }
            case OP_TYPE::UPDATE: {
                tid_word old_tid{wso_ptr->get_rec_ptr()->get_tidw_ref()};
                if (ce > old_tid.get_epoch()) {
                    Record* rec_ptr{wso_ptr->get_rec_ptr()};
                    // append new version
                    // gen new version
                    std::string vb{};
                    if (wso_ptr->get_op() != OP_TYPE::DELETE) {
                        wso_ptr->get_value(vb);
                    }
                    version* new_v{
                            new version(update_tid, vb, rec_ptr->get_latest())};

                    // update old version tid
                    tid_word old_version_tid{old_tid};
                    old_version_tid.set_latest(false);
                    old_version_tid.set_lock(false);
                    rec_ptr->get_latest()->set_tid(old_version_tid);

                    // set version to latest
                    rec_ptr->set_latest(new_v);
                } else {
                    // update existing version
                    if (wso_ptr->get_op() != OP_TYPE::DELETE) {
                        std::string vb{};
                        wso_ptr->get_value(vb);
                        wso_ptr->get_rec_ptr()->set_value(vb);
                    }
                }
                // detail info
                if (logging::get_enable_logging_detail_info()) {
                    VLOG(log_trace)
                            << log_location_prefix_detail_info
                            << "unlock key " +
                                       std::string(wso_ptr->get_rec_ptr()
                                                           ->get_key_view());
                }

                // unlock the record
                wso_ptr->get_rec_ptr()->set_tid(update_tid);

                // detail info
                if (logging::get_enable_logging_detail_info()) {
                    VLOG(log_trace)
                            << log_location_prefix_detail_info
                            << "unlocked key " +
                                       std::string(wso_ptr->get_rec_ptr()
                                                           ->get_key_view());
                }

                break;
            }
            default: {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "impossible code path.";
                return Status::ERR_FATAL;
            }
        }
#ifdef PWAL
        // add log records to local wal buffer
        std::string key{};
        wso_ptr->get_rec_ptr()->get_key(key);
        std::string val{};
        wso_ptr->get_value(val);
        log_operation lo{};
        switch (wso_ptr->get_op()) {
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
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "unknown operation type.";
                return Status::ERR_FATAL;
            }
        }
        lpwal::write_version_type::minor_write_version_type minor_version = 1;
        if (wso_ptr->get_storage() == storage::sequence_storage) {
            SequenceVersion version{};
            memcpy(&version, val.data(), sizeof(version));
            minor_version = version;
        } else {
            /**
             * To be larger than LTX.
             * Ltx's minor version is ltx id.
             */
            minor_version <<= 63; // NOLINT
            minor_version |= update_tid.get_tid();
        }
        ti->get_lpwal_handle().push_log(shirakami::lpwal::log_record(
                lo,
                lpwal::write_version_type(update_tid.get_epoch(),
                                          minor_version),
                wso_ptr->get_storage(), key, val));
#endif
        return Status::OK;
    };

#ifdef PWAL
    std::unique_lock<std::mutex> lk{ti->get_lpwal_handle().get_mtx_logs()};
#endif
    {
        std::shared_lock<std::shared_mutex> lk{ti->get_write_set().get_mtx()};
        if (ti->get_write_set().get_for_batch()) {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_bt()) {
                auto rc = process(&itr.second);
                if (rc == Status::OK) { continue; }
                if (rc == Status::ERR_FATAL) { return Status::ERR_FATAL; }
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "impossible code path.";
                return Status::ERR_FATAL;
            }
        } else {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
                auto rc = process(&itr);
                if (rc == Status::OK) { continue; }
                if (rc == Status::ERR_FATAL) { return Status::ERR_FATAL; }
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "impossible code path.";
                return Status::ERR_FATAL;
            }
        }
    }

    return Status::OK;
}

void compute_commit_tid(session* ti, epoch::epoch_t ce, tid_word& commit_tid) {
    // about tid
    auto tid_a{commit_tid};
    tid_a.inc_tid();

    // about linearizable for same worker
    auto tid_b{ti->get_mrc_tid()};
    tid_b.inc_tid();

    // about epoch
    tid_word tid_c{};
    tid_c.set_epoch(ce);

    commit_tid = std::max({tid_a, tid_b, tid_c});
    commit_tid.set_lock(false);
    commit_tid.set_absent(false);
    commit_tid.set_latest(true);
    commit_tid.set_by_short(true);

    ti->set_mrc_tid(commit_tid);
}

void register_point_read_by_short(session* const ti) {
    auto ce{ti->get_mrc_tid().get_epoch()};

    std::set<Record*> recs{};
    for (auto&& itr : ti->get_read_set_for_stx()) {
        recs.insert(itr.get_rec_ptr());
    }

    for (auto&& itr : recs) {
        auto& ro{itr->get_read_by()};
        ro.push(ce);
    }
}

void register_range_read_by_short(session* const ti) {
    auto ce{ti->get_mrc_tid().get_epoch()};

    {
        //take read lock
        std::shared_lock<std::shared_mutex> lk{
                ti->get_mtx_range_read_by_short_set()};
        for (auto&& itr : ti->get_range_read_by_short_set()) { itr->push(ce); }
    }
}

void process_tx_state(session* ti,
                      [[maybe_unused]] epoch::epoch_t durable_epoch) {
    if (ti->get_has_current_tx_state_handle()) {
// this tx state is checked
#ifdef PWAL
        ti->get_current_tx_state_ptr()->set_durable_epoch(durable_epoch);
        ti->get_current_tx_state_ptr()->set_kind(
                TxState::StateKind::WAITING_DURABLE);
#else
        ti->get_current_tx_state_ptr()->set_kind(TxState::StateKind::DURABLE);
#endif
    }
}

extern Status commit(session* const ti) {
    auto abort_and_call_ccb = [ti](Status rc) {
        short_tx::abort(ti);
        ti->call_commit_callback(rc, ti->get_result_info().get_reason_code(),
                                 0);
    };

    // write lock phase
    tid_word commit_tid{};
    auto rc{write_lock(ti, commit_tid)};
    if (rc != Status::OK) {
        abort_and_call_ccb(rc);
        return rc;
    }

    epoch::epoch_t ce{epoch::get_global_epoch()};

    // read wp verify
    rc = read_wp_verify(ti, ce, commit_tid);
    if (rc != Status::OK) {
        unlock_write_set(ti);
        abort_and_call_ccb(rc);
        return rc;
    }

    // node verify
    rc = ti->get_node_set().node_verify();
    if (rc != Status::OK) {
        unlock_write_set(ti);
        ti->set_result(reason_code::CC_OCC_PHANTOM_AVOIDANCE);
        abort_and_call_ccb(rc);
        return rc;
    }

    compute_commit_tid(ti, ce, commit_tid);

    // write phase
    rc = write_phase(ti, ce);
    if (rc != Status::OK) {
        if (rc == Status::ERR_FATAL) { return Status::ERR_FATAL; }
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "impossible code path.";
        return Status::ERR_FATAL;
    }

    // This calculation can be done outside the critical section.
    register_point_read_by_short(ti);
    register_range_read_by_short(ti);

    // sequence process
    // This must be after cc commit and before log process
    ti->commit_sequence(ti->get_mrc_tid());

    // flush log if need
#if defined(PWAL)
    auto oldest_log_epoch{ti->get_lpwal_handle().get_min_log_epoch()};
    // think the wal buffer is empty due to background thread's work
    if (oldest_log_epoch != 0 && // mean the wal buffer is not empty.
        oldest_log_epoch != epoch::get_global_epoch()) {
        // should flush
        shirakami::lpwal::flush_log(static_cast<void*>(ti));
    }
#endif

    auto this_dm = epoch::get_global_epoch();

    // about tx state
    process_tx_state(ti, this_dm);

    // clean up local set
    ti->clean_up();

    // set transaction result
    ti->set_result(reason_code::UNKNOWN);

    ti->call_commit_callback(rc, {}, this_dm);
    return Status::OK;
}

} // namespace shirakami::short_tx