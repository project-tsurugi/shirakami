#include <bitset>

#include "atomic_wrapper.h"

#include "include/helper.h"

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/garbage_manager.h"
#include "concurrency_control/silo/include/tuple_local.h" // sizeof(Tuple)

#include "shirakami/interface.h" // NOLINT

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

extern Status commit(Token token, commit_param* cp) { // NOLINT
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
    tid_word check;
    tid_word max_rset;
    for (auto&& itr : ti->get_read_set()) {
        const Record* rec_ptr = itr.get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw().get_obj());
        if ((itr.get_rec_read().get_tidw().get_epoch() != check.get_epoch() ||
             itr.get_rec_read().get_tidw().get_tid() != check.get_tid()) ||
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
    for (auto&& itr : ti->get_node_set()) {
        if (std::get<0>(itr) != std::get<1>(itr)->get_stable_version()) {
            ti->get_write_set().unlock();
            abort(token);
            return Status::ERR_PHANTOM;
        }
    }

    // Phase 4: Write & Unlock
    write_phase(ti, max_rset, max_wset,
                cp != nullptr ? cp->get_cp()
                              : commit_property::NOWAIT_FOR_COMMIT);

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
