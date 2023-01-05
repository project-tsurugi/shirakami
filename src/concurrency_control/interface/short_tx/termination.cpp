
#include "atomic_wrapper.h"
#include "storage.h"

#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/logging.h"

#include "glog/logging.h"

namespace shirakami::short_tx {

void unlock_write_set(session* const ti) {
    for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
        itr.get_rec_ptr()->get_tidw_ref().unlock();
    }
}


void unlock_not_insert_records(session* const ti,
                               std::size_t not_insert_locked_num) {
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        if (not_insert_locked_num == 0) { break; }
        auto* wso_ptr = &(elem);
        if (wso_ptr->get_op() == OP_TYPE::INSERT) { continue; }
        wso_ptr->get_rec_ptr()->get_tidw_ref().unlock();
        --not_insert_locked_num;
    }
}

void unlock_inserted_records(session* const ti) {
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        auto* wso_ptr = &(elem);
        Record* rec_ptr = wso_ptr->get_rec_ptr();
        if (wso_ptr->get_op() == OP_TYPE::INSERT) {
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
                    tid.set_epoch(ti->get_step_epoch());
                    rec_ptr->set_tid(tid); // and unlock
                } else {
                    rec_ptr->get_tidw_ref().unlock();
                }
            }
        }
    }
}

Status abort(session* ti) { // NOLINT
    // about tx state
    ti->set_tx_state_if_valid(TxState::StateKind::ABORTED);

    // about inserted records
    unlock_inserted_records(ti);

    ti->clean_up();
    return Status::OK;
}

Status read_verify(session* ti, Storage const storage, tid_word read_tid,
                   tid_word check, Record* const rec_ptr) {
    if (read_tid.get_tid() != check.get_tid() ||
        read_tid.get_epoch() != check.get_epoch() || check.get_absent() ||
        (check.get_lock() && ti->get_write_set().search(rec_ptr) == nullptr)) {
        ti->get_result_info().set_key_storage_name(rec_ptr->get_key_view(),
                                                   storage);
        return Status::ERR_CC;
    }
    return Status::OK;
}

Status wp_verify(Storage const st, epoch::epoch_t const commit_epoch) {
    wp::wp_meta* wm{};
    auto rc{find_wp_meta(st, wm)};
    if (rc != Status::OK) { LOG(ERROR); }
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
    for (auto&& itr : ti->get_read_set()) {
        auto* rec_ptr = itr.get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw_ref().get_obj());

        // verify
        // ==============================
        if (read_verify(ti, itr.get_storage(), itr.get_tid(), check, rec_ptr) !=
            Status::OK) {
            unlock_write_set(ti);
            short_tx::abort(ti);
            ti->set_result(reason_code::CC_OCC_READ_VERIFY);
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

Status upsert_process_at_write_lock(session* ti, write_set_obj* wso,
                                    std::size_t& not_inserted_lock) {
    // check key exists yet
    std::string key{};
    wso->get_rec_ptr()->get_key(key);
RETRY: // NOLINT
    Record* rec_ptr{};
    auto rc = get<Record>(wso->get_storage(), key, rec_ptr);
    if (rc == Status::OK) {
        // point (*1)
        // hooked yet
        if (wso->get_rec_ptr() != rec_ptr) {
            // changed from read phase
            wso->set_rec_ptr(rec_ptr);
        }
        // check ts
        tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        rec_ptr->get_tidw_ref().lock();
        check = loadAcquire(rec_ptr->get_tidw_ref().get_obj());
        if ((check.get_latest() && check.get_absent())) {
            // inserting state
            return Status::OK;
        }
        if (!check.get_absent()) {
            // normal state
            ++not_inserted_lock;
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
    std::size_t not_insert_locked_num{0};
    // dead lock avoidance
    auto& cont_for_occ = ti->get_write_set().get_ref_cont_for_occ();
    std::sort(cont_for_occ.begin(), cont_for_occ.end());
    for (auto&& elem : cont_for_occ) {
        auto* wso_ptr = &(elem);
        auto* rec_ptr{wso_ptr->get_rec_ptr()};
        auto abort_process = [ti, &not_insert_locked_num]() {
            if (not_insert_locked_num > 0) {
                unlock_not_insert_records(ti, not_insert_locked_num);
            }
            short_tx::abort(ti);
        };
        if (wso_ptr->get_op() == OP_TYPE::INSERT) {
            rec_ptr->get_tidw_ref().lock();
            tid_word tid{rec_ptr->get_tidw_ref()};
            if (tid.get_latest() && !tid.get_absent()) {
                // the record is existing record (not inserting, deleted)
                rec_ptr->get_tidw_ref().unlock();
                abort_process();
                ti->get_result_info().set_reason_code(reason_code::KVS_INSERT);
                ti->get_result_info().set_key_storage_name(
                        rec_ptr->get_key_view(), wso_ptr->get_storage());
                return Status::ERR_KVS;
            }
        } else if (wso_ptr->get_op() == OP_TYPE::UPSERT) {
            auto rc = upsert_process_at_write_lock(ti, wso_ptr,
                                                   not_insert_locked_num);
            if (rc == Status::OK) {
                // may change op type, so should do continue explicitly.
                continue;
            }
            if (rc == Status::ERR_CC) {
                abort_process();
                return Status::ERR_CC;
            }
        } else if (wso_ptr->get_op() == OP_TYPE::UPDATE ||
                   wso_ptr->get_op() == OP_TYPE::DELETE) {
            rec_ptr->get_tidw_ref().lock();
            commit_tid = std::max(commit_tid, rec_ptr->get_tidw_ref());
            ++not_insert_locked_num;
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
            LOG(ERROR) << log_location_prefix << "programming error";
            return Status::ERR_FATAL;
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
                wso_ptr->get_rec_ptr()->set_tid(update_tid);
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
                LOG(ERROR) << log_location_prefix << "programming error";
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
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        auto rc{process(&elem)};
        if (rc == Status::OK) { continue; }
        if (rc == Status::ERR_FATAL) { return Status::ERR_FATAL; }
        LOG(ERROR) << log_location_prefix << "impossible code path.";
        return Status::ERR_FATAL;
    }

    return Status::OK;
}

Status node_verify(session* ti) {
    for (auto&& itr : ti->get_node_set()) {
        if (std::get<0>(itr) != std::get<1>(itr)->get_stable_version()) {
            unlock_write_set(ti);
            short_tx::abort(ti);
            return Status::ERR_CC;
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
    ti->set_mrc_tid(commit_tid);
}

void register_point_read_by_short(session* const ti) {
    auto ce{ti->get_mrc_tid().get_epoch()};

    std::set<Record*> recs{};
    for (auto&& itr : ti->get_read_set()) { recs.insert(itr.get_rec_ptr()); }

    for (auto&& itr : recs) {
        auto& ro{itr->get_read_by()};
        ro.push(ce);
    }
}

void register_range_read_by_short(session* const ti) {
    auto ce{ti->get_mrc_tid().get_epoch()};

    for (auto&& itr : ti->get_range_read_by_short_set()) { itr->push(ce); }
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
        return rc;
    }

    epoch::epoch_t ce{epoch::get_global_epoch()};

    // read wp verify
    rc = read_wp_verify(ti, ce, commit_tid);
    if (rc != Status::OK) {
        unlock_write_set(ti);
        short_tx::abort(ti);
        return rc;
    }

    // node verify
    rc = node_verify(ti);
    if (rc != Status::OK) {
        unlock_write_set(ti);
        short_tx::abort(ti);
        ti->set_result(reason_code::CC_OCC_PHANTOM_AVOIDANCE);
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

    // about tx state
    process_tx_state(ti, ce);

    // clean up local set
    ti->clean_up();

    // set transaction result
    ti->set_result(reason_code::UNKNOWN);

    return Status::OK;
}

} // namespace shirakami::short_tx