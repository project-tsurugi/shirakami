
#include "atomic_wrapper.h"
#include "storage.h"

#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
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

void change_inserting_records_state(session* const ti) {
    auto process = [ti](write_set_obj* wso_ptr) {
        Record* rec_ptr = wso_ptr->get_rec_ptr();
        if (wso_ptr->get_op() == OP_TYPE::INSERT ||
            wso_ptr->get_op() == OP_TYPE::UPSERT) {
            tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
            // pre-check
            if (check.get_latest() && check.get_absent()) {
                rec_ptr->get_tidw_ref().lock();
                check = loadAcquire(rec_ptr->get_tidw_ref().get_obj());

                // detail info
                if (logging::get_enable_logging_detail_info()) {
                    VLOG(log_trace)
                            << log_location_prefix_detail_info
                            << "unlock key " +
                                       std::string(rec_ptr->get_key_view());
                }

                // main-check
                if (check.get_latest() && check.get_absent()) {
                    // inserting yet
                    tid_word tid{};
                    tid.set_absent(true);
                    tid.set_latest(false);
                    tid.set_lock(false);
                    tid.set_epoch(ti->get_step_epoch());
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

Status read_verify(session* ti, Storage const storage, tid_word read_tid,
                   tid_word check, const Record* const rec_ptr) {
    if (
            // different tid
            read_tid.get_tid() != check.get_tid() ||
            // different epoch
            read_tid.get_epoch() != check.get_epoch() ||
            // locked and it's not own.
            (check.get_lock() &&
             ti->get_write_set().search(rec_ptr) == nullptr)) {
        ti->get_result_info().set_key_storage_name(rec_ptr->get_key_view(),
                                                   storage);
        return Status::ERR_CC;
    }
    return Status::OK;
}

Status wp_verify(Storage const st, epoch::epoch_t const commit_epoch) {
    wp::wp_meta* wm{};
    auto rc{find_wp_meta(st, wm)};
    if (rc != Status::OK) {
        LOG(ERROR) << "unreachable path. It strongly suspect that DML and DDL "
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
        if (read_verify(ti, itr.get_storage(), itr.get_tid(), check, rec_ptr) !=
            Status::OK) {
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

Status upsert_process_at_write_lock(session* ti, write_set_obj* wso) {
    // check key exists yet
    std::string key{};
    wso->get_rec_ptr()->get_key(key);
RETRY: // NOLINT
    Record* rec_ptr{};
    auto rc = get<Record>(wso->get_storage(), key, rec_ptr);
    if (rc == Status::OK) {
        // the key exists and is hooked.
        // point (*1)
        if (wso->get_rec_ptr() != rec_ptr) {
            LOG(ERROR) << "The record pointer shouldn't be changed from read "
                          "phase.";
            return Status::ERR_CC;
        }
        // check ts
        tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};

        // detail info
        if (logging::get_enable_logging_detail_info()) {
            VLOG(log_trace)
                    << log_location_prefix_detail_info
                    << "lock key " + std::string(rec_ptr->get_key_view());
        }

        // locking
        rec_ptr->get_tidw_ref().lock();

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
            // inserting state
            return Status::OK;
        }
        // it is deleted state
        // change it to inserting state.
        check.set_latest(true);
        rec_ptr->set_tid(check);

        // check it is hooked yet
        auto rc = get<Record>(wso->get_storage(), key, rec_ptr);
        auto cleanup_old_process = [wso](tid_word check) {
            check.set_latest(false);
            check.set_absent(true);
            check.set_lock(false);
            wso->get_rec_ptr()->set_tid(check);
        };
        if (rc == Status::OK) {
            // some key hit
            if (wso->get_rec_ptr() == rec_ptr) {
                // success converting deleted to inserted
                return Status::OK;
            }
            // converting record is unhooked by gc
            cleanup_old_process(check);
            goto RETRY; // NOLINT
        } else {
            // no key hit
            // point (*2)
            // gced in range from point (*1) to point (*2)
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

        // detail info
        if (logging::get_enable_logging_detail_info()) {
            VLOG(log_trace) << log_location_prefix_detail_info
                            << "inserting locked record, key " +
                                       std::string(rec_ptr->get_key_view());
        }

        rec_ptr->set_tid(tid);
        yakushima::node_version64* nvp{};
        if (yakushima::status::OK == put<Record>(ti->get_yakushima_token(),
                                                 wso->get_storage(), key,
                                                 rec_ptr, nvp)) {
            Status check_node_set_res{ti->update_node_set(nvp)};
            if (check_node_set_res == Status::ERR_CC) {
                /**
                  * This This transaction is confirmed to be aborted 
                  * because the previous scan was destroyed by an insert
                  * by another transaction.
                  */
                ti->get_result_info().set_reason_code(
                        reason_code::CC_OCC_PHANTOM_AVOIDANCE);
                ti->get_result_info().set_key_storage_name(key,
                                                           wso->get_storage());
                return Status::ERR_CC;
            }
            // success inserting

            // detail info
            if (logging::get_enable_logging_detail_info()) {
                VLOG(log_trace) << log_location_prefix_detail_info
                                << "inserted locking key " +
                                           std::string(rec_ptr->get_key_view());
            }

            return Status::OK;
        }
        // else insert_result == Status::WARN_ALREADY_EXISTS
        // so retry from index access
        delete rec_ptr; // NOLINT
        goto RETRY;     // NOLINT
    }

    return Status::OK; // todo
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
        if (wso_ptr->get_op() == OP_TYPE::INSERT) {
            // detail info
            if (logging::get_enable_logging_detail_info()) {
                VLOG(log_trace)
                        << log_location_prefix_detail_info
                        << "lock key " + std::string(rec_ptr->get_key_view());
            }

            // lock the record
            ++num_locked;
            rec_ptr->get_tidw_ref().lock();

            // detail info
            if (logging::get_enable_logging_detail_info()) {
                VLOG(log_trace)
                        << log_location_prefix_detail_info
                        << "locked key " + std::string(rec_ptr->get_key_view());
            }

            tid_word tid{rec_ptr->get_tidw_ref()};
            if (tid.get_latest() && !tid.get_absent()) {
                // the record is existing record (not inserting, deleted)
                abort_process();
                ti->get_result_info().set_reason_code(reason_code::KVS_INSERT);
                ti->get_result_info().set_key_storage_name(
                        rec_ptr->get_key_view(), wso_ptr->get_storage());
                return Status::ERR_KVS;
            }
        } else if (wso_ptr->get_op() == OP_TYPE::UPSERT) {
            auto rc = upsert_process_at_write_lock(ti, wso_ptr);
            if (rc == Status::OK) {
                // may change op type, so should do continue explicitly.
                ++num_locked;
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
            LOG(ERROR) << log_location_prefix << "unreachable path";
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
        VLOG(log_trace) << "write. op type: " << wso_ptr->get_op()
                        << ", key: " << wso_ptr->get_rec_ptr()->get_key_view()
                        << ", value: " << wso_ptr->get_value_view();
        switch (wso_ptr->get_op()) {
            case OP_TYPE::UPSERT:
            case OP_TYPE::INSERT: {
                tid_word old_tid{wso_ptr->get_rec_ptr()->get_tidw_ref()};
                if (old_tid.get_latest() && old_tid.get_absent()) {
                    // set value
                    std::string vb{};
                    wso_ptr->get_value(vb);
                    wso_ptr->get_rec_ptr()->set_value(vb);

                    // set timestamp
                    wso_ptr->get_rec_ptr()->set_tid(update_tid);
                    break;
                }
                [[fallthrough]];
                // upsert is update
            }
            case OP_TYPE::DELETE: {
                if (wso_ptr->get_op() == OP_TYPE::DELETE) {
                    update_tid.set_absent(true);
                    update_tid.set_latest(false);
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
                LOG(ERROR) << log_location_prefix << "impossible code path.";
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
                LOG(ERROR) << log_location_prefix << "unknown operation type.";
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
                LOG(ERROR) << log_location_prefix << "impossible code path.";
                return Status::ERR_FATAL;
            }
        } else {
            for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
                auto rc = process(&itr);
                if (rc == Status::OK) { continue; }
                if (rc == Status::ERR_FATAL) { return Status::ERR_FATAL; }
                LOG(ERROR) << log_location_prefix << "impossible code path.";
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
    // write lock phase
    tid_word commit_tid{};
    auto rc{write_lock(ti, commit_tid)};
    if (rc != Status::OK) {
        short_tx::abort(ti);
        ti->call_commit_callback(rc, ti->get_result_info().get_reason_code(),
                                 0);
        return rc;
    }

    epoch::epoch_t ce{epoch::get_global_epoch()};

    // read wp verify
    rc = read_wp_verify(ti, ce, commit_tid);
    if (rc != Status::OK) {
        unlock_write_set(ti);
        short_tx::abort(ti);
        ti->call_commit_callback(rc, ti->get_result_info().get_reason_code(),
                                 0);
        return rc;
    }

    // node verify
    rc = ti->get_node_set().node_verify();
    if (rc != Status::OK) {
        unlock_write_set(ti);
        short_tx::abort(ti);
        ti->set_result(reason_code::CC_OCC_PHANTOM_AVOIDANCE);
        ti->call_commit_callback(rc, ti->get_result_info().get_reason_code(),
                                 0);
        return rc;
    }

    compute_commit_tid(ti, ce, commit_tid);

    // write phase
    rc = write_phase(ti, ce);
    if (rc != Status::OK) {
        if (rc == Status::ERR_FATAL) { return Status::ERR_FATAL; }
        LOG(ERROR) << log_location_prefix << "impossible code path.";
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