#include <bitset>

#include "atomic_wrapper.h"

#include "include/helper.h"

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/garbage_manager.h"
#include "concurrency_control/silo/include/snapshot_manager.h"

#include "concurrency_control/include/tuple_local.h" // sizeof(Tuple)

#include "shirakami/interface.h" // NOLINT

#include "glog/logging.h"

namespace shirakami {

void clean_up_session_flag(session* ti) {
    ti->set_read_only(false);
    ti->set_tx_began(false);
}

void clean_up_session_info(session* ti) {
    ti->clean_up_local_set();
    ti->clean_up_scan_caches();
    clean_up_session_flag(ti);
}

Status abort(Token token) { // NOLINT
    auto* ti = static_cast<session*>(token);

    ti->get_write_set().remove_inserted_records_from_yakushima(
            token, ti->get_yakushima_token());

    clean_up_session_info(ti);

    return Status::OK;
}

Status write_lock(Token token, tid_word& max_wset) {
    auto* ti = static_cast<session*>(token);

    // Phase 1: Sort lock list;
    ti->get_write_set().sort_if_ol();

    // Phase 2: Lock write set;
    auto process = [token, ti, &max_wset](write_set_obj* we_ptr,
                                          std::size_t ctr) {
        // update/delete
        we_ptr->get_rec_ptr()->get_tidw().lock();
        ++ctr;
        if ((we_ptr->get_op() == OP_TYPE::UPDATE ||
             we_ptr->get_op() == OP_TYPE::DELETE) && // NOLINT
            we_ptr->get_rec_ptr()->get_tidw().get_absent()) {
            ti->get_write_set().unlock(ctr);
            abort(token);
            return Status::ERR_WRITE_TO_DELETED_RECORD;
        }

#if defined(CPR)
        // cpr verify
        if (ti->get_phase() == cpr::phase::REST &&
            we_ptr->get_rec_ptr()->get_version() > ti->get_version()) {
            ti->get_write_set().unlock(ctr);
            abort(token);
            return Status::ERR_CPR_ORDER_VIOLATION;
        }
#endif

        max_wset = std::max(max_wset, we_ptr->get_rec_ptr()->get_tidw());
        return Status::OK;
    };

    std::size_t ctr{0};
    if (ti->get_write_set().get_for_batch()) {
        for (auto&& elem : ti->get_write_set().get_cont_for_bt()) {
            write_set_obj* we_ptr = &std::get<1>(elem);
            if (we_ptr->get_op() != OP_TYPE::INSERT) {
                auto rc = process(we_ptr, ctr);
                if (rc != Status::OK) { return rc; }
            }
        }
    } else {
        for (auto&& elem : ti->get_write_set().get_cont_for_ol()) {
            write_set_obj* we_ptr = &(elem);
            if (we_ptr->get_op() != OP_TYPE::INSERT) {
                auto rc = process(we_ptr, ctr);
                if (rc != Status::OK) { return rc; }
            }
        }
    }

    return Status::OK;
}

Status read_verify(Token token, tid_word& max_rset) {
    auto* ti = static_cast<session*>(token);

    tid_word check;
    for (auto&& itr : ti->get_read_set()) {
        auto& rsobj = itr;
        const Record* rec_ptr = rsobj.get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw().get_obj());
        if ((rsobj.get_read_tid_ref().get_epoch() != check.get_epoch() ||
             rsobj.get_read_tid_ref().get_tid() != check.get_tid()) ||
            check.get_absent() // check whether it was deleted.
            || (check.get_lock() &&
                (ti->get_write_set().search(const_cast<Record*>(rec_ptr)) ==
                 nullptr))) {
            ti->get_write_set().unlock();
            abort(token);

            return Status::ERR_VALIDATION;
        }
        max_rset = std::max(max_rset, check);
    }

    // node verify for protect phantom
    return Status::OK;
}

Status node_verify(Token token) {
    auto* ti = static_cast<session*>(token);
    for (auto&& itr : ti->get_node_set()) {
        if (std::get<0>(itr) != std::get<1>(itr)->get_stable_version()) {
            ti->get_write_set().unlock();
            abort(token);
            return Status::ERR_PHANTOM;
        }
    }

    // Phase 4: Write & Unlock
    return Status::OK;
}

#if defined(PWAL) || defined(CPR)
void update_commit_param(session* ti, commit_param* cp) {
#if defined(PWAL)
    if (cp != nullptr) cp->set_ctid(ti->get_mrctid().get_obj());
#elif defined(CPR)
    if (cp != nullptr) {
        cpr::phase_version current_gpv = cpr::global_phase_version::get_gpv();
        if (ti->get_phase() == current_gpv.get_phase() &&
            current_gpv.get_phase() == cpr::phase::REST) {
            cp->set_ctid(ti->get_version());
        } else {
            /**
             * cpr's logical consistency point is between rest phase and in-progress phase.
             * If tx beginning points and ending points are globally rest phase, it is before consistency point,
             * otherwise after the point.
             */
            cp->set_ctid(ti->get_version() + 1);
        }
    }
#endif
}
#endif

void write_phase(session* const ti, const tid_word& max_r_set,
                 const tid_word& max_w_set,
                 [[maybe_unused]] commit_property cp) {
    /*
     * It calculates the smallest number that is
     * (a) larger than the TID of any record read or written by the transaction,
     * (b) larger than the worker's most recently chosen TID,
     * and (C) in the current global epoch.
     */
    tid_word tid_a;
    tid_word tid_b;
    tid_word tid_c;

    /*
     * calculates (a)
     * about read_set
     */
    tid_a = std::max(max_w_set, max_r_set);
    tid_a.inc_tid();

    /*
     * calculates (b)
     * larger than the worker's most recently chosen TID,
     */
    tid_b = ti->get_mrctid();
    tid_b.inc_tid();

    /* calculates (c) */
    tid_c.set_epoch(ti->get_epoch());

    /* compare a, b, c */
    tid_word max_tid = std::max({tid_a, tid_b, tid_c});
    max_tid.set_lock(false);
    max_tid.set_absent(false);
    max_tid.set_latest(true);
    max_tid.set_epoch(ti->get_epoch());
    ti->set_mrc_tid(max_tid);

#ifdef PWAL
    ti->pwal(max_tid.get_obj(), cp);
#endif

    auto process = [ti, max_tid](write_set_obj* we_ptr) {
        Record* rec_ptr = we_ptr->get_rec_ptr();
#ifdef CPR
        std::string value{};
        we_ptr->get_value(value);
        ti->regi_diff_upd_set(we_ptr->get_storage(), max_tid, we_ptr->get_op(),
                              rec_ptr, value);
#endif
        auto safely_snap_work = [&rec_ptr, &ti] {
            if (snapshot_manager::get_snap_epoch(ti->get_epoch()) !=
                snapshot_manager::get_snap_epoch(
                        rec_ptr->get_tidw().get_epoch())) {
                // update safely snap
                std::string key{};
                rec_ptr->get_tuple().get_key(key);
                std::string val{};
                rec_ptr->get_tuple().get_value(val);
                auto* new_rec = // NOLINT
                        new Record(key, val);
                new_rec->get_tidw().set_epoch(rec_ptr->get_tidw().get_epoch());
                new_rec->get_tidw().set_latest(true);
                new_rec->get_tidw().set_lock(false);
                new_rec->get_tidw().set_absent(false);
                if (rec_ptr->get_snap_ptr() == nullptr) {
                    // create safely snap
                    rec_ptr->set_snap_ptr(new_rec);
                } else {
                    // create safely snap and insert at second.
                    new_rec->set_snap_ptr(rec_ptr->get_snap_ptr());
                    rec_ptr->set_snap_ptr(new_rec);
                }
                ti->get_gc_handle().get_snap_cont().push(
                        std::make_pair(ti->get_epoch(), new_rec));
            }
        };
        switch (we_ptr->get_op()) {
            case OP_TYPE::INSERT: {
#ifdef CPR
                /**
                 * When this transaction started in the rest phase, cpr thread never started scanning.
                 */
                if (ti->get_phase() == cpr::phase::REST) {
                    /**
                     * If this transaction have a serialization point after the checkpoint boundary and before the scan
                     * of the cpr thread, the cpr thread will include it even though it should not be included in the
                     * checkpoint.
                     * Since this transaction inserted this record with a lock, it is guaranteed that the checkpoint
                     * thread has never been reached.
                     */
                    rec_ptr->set_version(ti->get_version());
                } else {
                    rec_ptr->set_version(ti->get_version() + 1);
                }
#endif
                storeRelease(rec_ptr->get_tidw().get_obj(), max_tid.get_obj());
                break;
            }
            case OP_TYPE::UPDATE: {
                safely_snap_work();
#ifdef CPR
                // update about cpr
                if (ti->get_phase() != cpr::phase::REST &&
                    rec_ptr->get_version() != (ti->get_version() + 1)) {
                    rec_ptr->get_stable() = rec_ptr->get_tuple();
                    rec_ptr->set_version(ti->get_version() + 1);
                }
#endif
                // update value
                std::string new_value{};
                we_ptr->get_value(new_value);
                rec_ptr->set_value(new_value);
                storeRelease(rec_ptr->get_tidw().get_obj(), max_tid.get_obj());
                break;
            }
            case OP_TYPE::DELETE: {
                safely_snap_work();
                tid_word delete_tid = max_tid;
                delete_tid.set_latest(false);
                delete_tid.set_absent(true);

#ifdef CPR
                if (ti->get_phase() != cpr::phase::REST) {
                    /**
                     * This is in checkpointing phase (in-progress or wait-flush), meaning checkpoint thread may be scanning.
                     */
                    if (ti->get_version() + 1 != rec_ptr->get_version()) {
                        rec_ptr->get_stable() = rec_ptr->get_tuple();
                        rec_ptr->set_version(ti->get_version() + 1);
                    }
                } else {
                    /**
                     * Used by the manager as information about when memory can be freed.
                     */
                    rec_ptr->set_version(ti->get_version());
                }
#endif
                storeRelease(rec_ptr->get_tidw().get_obj(),
                             delete_tid.get_obj());
                // for delayed unhook
                ti->get_cleanup_handle().push(
                        {std::string(we_ptr->get_storage()), rec_ptr});
                break;
            }
            default:
                LOG(FATAL) << "unknown operation type.";
        }
    };

    if (ti->get_write_set().get_for_batch()) {
        for (auto&& elem : ti->get_write_set().get_cont_for_bt()) {
            process(&std::get<1>(elem));
        }
    } else {
        for (auto&& elem : ti->get_write_set().get_cont_for_ol()) {
            process(&elem);
        }
    }
}

Status commit(Token token, commit_param* cp) { // NOLINT
    auto* ti = static_cast<session*>(token);

    tid_word max_wset;
    auto rc = write_lock(token, max_wset);
    if (rc != Status::OK) { return rc; }

    // Serialization point
    asm volatile("" ::: "memory"); // NOLINT
    /**
     * In x86/64, the write-read order between different addresses is not guaranteed.
     */
    std::atomic_thread_fence(std::memory_order_release);
    ti->set_epoch(epoch::get_global_epoch());
    asm volatile("" ::: "memory"); // NOLINT
    /**
     * In x86/64, the order between reads (epoch read and read verify) is guaranteed.
     */

    // Phase 3: Validation
    tid_word max_rset;
    rc = read_verify(token, max_rset);
    if (rc != Status::OK) { return rc; }

    rc = node_verify(token);
    if (rc != Status::OK) { return rc; }

    write_phase(ti, max_rset, max_wset,
                cp != nullptr ? cp->get_cp()
                              : commit_property::NOWAIT_FOR_COMMIT);

#if defined(PWAL) || defined(CPR)
    update_commit_param(ti, cp);
#endif

    clean_up_session_info(ti);

    return Status::OK;
}

extern bool check_commit(Token token,
                         [[maybe_unused]] std::uint64_t commit_id) { // NOLINT
    [[maybe_unused]] auto* ti = static_cast<session*>(token);
#if defined(PWAL)
    return ti->get_flushed_ctid().get_obj() > commit_id;
#elif defined(CPR)
    return commit_id < cpr::global_phase_version::get_gpv().get_version();
#else
    /**
     * No logging method means pre-commit is commit.
     */
    return true;
#endif
}

} // namespace shirakami
