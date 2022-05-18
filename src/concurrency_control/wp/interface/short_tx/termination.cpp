
#include "atomic_wrapper.h"

#include "concurrency_control/wp/interface/short_tx/include/short_tx.h"

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/include/tuple_local.h"

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
        if (wso_ptr->get_op() == OP_TYPE::INSERT) {
            tid_word tid{};
            tid.set_absent(true);
            tid.set_latest(false);
            tid.set_lock(false);
            tid.set_epoch(ti->get_step_epoch());
            wso_ptr->get_rec_ptr()->set_tid(tid);
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

Status read_verify(session* ti, tid_word read_tid, tid_word check,
                   Record* const rec_ptr) {
    if (read_tid.get_tid() != check.get_tid() ||
        read_tid.get_epoch() != check.get_epoch() || check.get_absent() ||
        (check.get_lock() && ti->get_write_set().search(rec_ptr) == nullptr)) {
        return Status::ERR_VALIDATION;
    }
    return Status::OK;
}

Status wp_verify(Storage const st, epoch::epoch_t const read_epoch,
                 epoch::epoch_t const commit_epoch) {
    wp::wp_meta* wm{};
    auto rc{find_wp_meta(st, wm)};
    if (rc != Status::OK) { LOG(FATAL); }
    auto wps{wm->get_wped()};
    auto find_min_ep{wp::wp_meta::find_min_ep(wps)};
    if (find_min_ep != 0 &&
        (find_min_ep <= read_epoch || find_min_ep <= commit_epoch)) {
        return Status::ERR_VALIDATION;
    }
    return Status::OK;
}

Status read_wp_verify(session* const ti, epoch::epoch_t ce,
                      tid_word& commit_tid) {
    tid_word check{};
    for (auto&& itr : ti->get_read_set()) {
        auto* rec_ptr = itr.get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw_ref().get_obj());

        // verify
        // ==============================
        if (read_verify(ti, itr.get_tid(), check, rec_ptr) != Status::OK) {
            unlock_write_set(ti);
            short_tx::abort(ti);
            return Status::ERR_VALIDATION;
        }

        if (wp_verify(itr.get_storage(), itr.get_tid().get_epoch(), ce) !=
            Status::OK) {
            unlock_write_set(ti);
            short_tx::abort(ti);
            return Status::ERR_CONFLICT_ON_WRITE_PRESERVE;
        }
        // ==============================

        commit_tid = std::max(check, commit_tid);
    }

    return Status::OK;
}

Status write_lock(session* ti, tid_word& commit_tid) {
    std::size_t not_insert_locked_num{0};
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        auto* wso_ptr = &(elem);
        if (wso_ptr->get_op() != OP_TYPE::INSERT) {
            auto* rec_ptr{wso_ptr->get_rec_ptr()};
            rec_ptr->get_tidw_ref().lock();
            commit_tid = std::max(commit_tid, rec_ptr->get_tidw_ref());
            ++not_insert_locked_num;
            if ((wso_ptr->get_op() == OP_TYPE::UPDATE ||
                 wso_ptr->get_op() == OP_TYPE::DELETE) &&
                rec_ptr->get_tidw_ref().get_absent()) {
                if (not_insert_locked_num > 0) {
                    unlock_not_insert_records(ti, not_insert_locked_num);
                }
                short_tx::abort(ti);
                return Status::ERR_WRITE_TO_DELETED_RECORD;
            }
        }
    }

    return Status::OK;
}

Status write_phase(session* ti, epoch::epoch_t ce) {
    auto process = [ti, ce](write_set_obj* wso_ptr) {
        tid_word update_tid{ti->get_mrc_tid()};
        switch (wso_ptr->get_op()) {
            case OP_TYPE::INSERT: {
                wso_ptr->get_rec_ptr()->set_tid(update_tid);
                break;
            }
            case OP_TYPE::DELETE: {
                update_tid.set_absent(true);
                update_tid.set_latest(false);
                [[fallthrough]];
            }
            case OP_TYPE::UPDATE:
            case OP_TYPE::UPSERT: {
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
                    std::string vb{};
                    if (wso_ptr->get_op() != OP_TYPE::DELETE) {
                        wso_ptr->get_value(vb);
                    }
                    wso_ptr->get_rec_ptr()->set_value(vb);
                }
                wso_ptr->get_rec_ptr()->set_tid(update_tid);
                break;
            }
            default: {
                LOG(ERROR) << "impossible code path.";
                return Status::ERR_FATAL;
            }
        }
        return Status::OK;
    };

    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        auto rc{process(&elem)};
        if (rc == Status::OK) { continue; }
        if (rc == Status::ERR_FATAL) { return Status::ERR_FATAL; }
        LOG(ERROR) << "impossible code path.";
        return Status::ERR_FATAL;
    }

    return Status::OK;
}

Status node_verify(session* ti) {
    for (auto&& itr : ti->get_node_set()) {
        if (std::get<0>(itr) != std::get<1>(itr)->get_stable_version()) {
            unlock_write_set(ti);
            short_tx::abort(ti);
            return Status::ERR_PHANTOM;
        }
    }
    return Status::OK;
}

void compute_commit_tid(session* ti, epoch::epoch_t ce, tid_word& commit_tid) {
    auto tid_a{commit_tid};
    tid_a.inc_tid();

    auto tid_b{ti->get_mrc_tid()};
    tid_b.inc_tid();

    tid_word tid_c{};
    tid_c.set_epoch(ce);

    commit_tid = std::max({tid_a, tid_b, tid_c});
    commit_tid.set_lock(false);
    commit_tid.set_absent(false);
    commit_tid.set_latest(true);
    ti->set_mrc_tid(commit_tid);
}

void register_read_by_occ(session* const ti) {
    auto ce{ti->get_mrc_tid().get_epoch()};

    std::vector<Record*> recs{};
    for (auto&& itr : ti->get_read_set()) {
        recs.emplace_back(itr.get_rec_ptr());
    }

    /**
     * Since the registration of read_by involves an exclusive lock, the 
     * redundancy is deleted. 
     */
    std::sort(recs.begin(), recs.end());
    recs.erase(std::unique(recs.begin(), recs.end()), recs.end());
    for (auto&& itr : recs) {
        auto& ro{itr->get_read_by()};
        ro.push(ce);
    }
}

extern Status commit(session* ti, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
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
        return rc;
    }

    compute_commit_tid(ti, ce, commit_tid);

    // write phase
    rc = write_phase(ti, ce);
    if (rc != Status::OK) {
        if (rc == Status::ERR_FATAL) { return Status::ERR_FATAL; }
        LOG(ERROR) << "impossible code path.";
        return Status::ERR_FATAL;
    }

    // This calculation can be done outside the critical section.
    register_read_by_occ(ti);

    // about tx state
    // this should be before clean_up func
    // todo think logging
    ti->set_tx_state_if_valid(TxState::StateKind::DURABLE);

    // clean up local set
    ti->clean_up();

    return Status::OK;
}

} // namespace shirakami::short_tx