#include <bitset>

#include "atomic_wrapper.h"

#ifdef CC_SILO_VARIANT

#include "concurrency_control/silo_variant/include/epoch.h"
#include "concurrency_control/silo_variant/include/garbage_collection.h"
#include "concurrency_control/silo_variant/include/interface_helper.h"

#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#endif                            // INDEX_KOHLER_MASSTREE

#include "include/tuple_local.h"  // sizeof(Tuple)
#include "kvs/interface.h"        // NOLINT

namespace shirakami::cc_silo_variant {

Status abort(Token token) {  // NOLINT
    auto* ti = static_cast<cc_silo_variant::session_info*>(token);
#if defined(INDEX_KOHLER_MASSTREE) || defined(INDEX_YAKUSHIMA)
    ti->remove_inserted_records_of_write_set_from_masstree();
#endif
    ti->clean_up_ops_set();
    ti->clean_up_scan_caches();
    ti->set_tx_began(false);
    ti->gc_records_and_values();
    return Status::OK;
}

extern Status commit(Token token, commit_param* cp) {  // NOLINT
    auto* ti = static_cast<cc_silo_variant::session_info*>(token);
    cc_silo_variant::tid_word max_rset;
    cc_silo_variant::tid_word max_wset;

    // Phase 1: Sort lock list;
    std::sort(ti->get_write_set().begin(), ti->get_write_set().end());

    // Phase 2: Lock write set;
    for (auto itr = ti->get_write_set().begin(); itr != ti->get_write_set().end();
         ++itr) {
        if (itr->get_op() == OP_TYPE::INSERT) continue;
        // after this, update/delete
        itr->get_rec_ptr()->get_tidw().lock();
        if (itr->get_op() == OP_TYPE::UPDATE &&  // NOLINT
            itr->get_rec_ptr()->get_tidw().get_absent()) {
            ti->unlock_write_set(ti->get_write_set().begin(), itr + 1);
            abort(token);
            return Status::ERR_WRITE_TO_DELETED_RECORD;
        }

#if defined(CPR)
        // cpr verify
        if (ti->get_phase() == cpr::phase::REST && itr->get_rec_ptr()->get_version() > ti->get_version()) {
            ti->unlock_write_set(ti->get_write_set().begin(), itr + 1);
            abort(token);
            return Status::ERR_WRITE_TO_DELETED_RECORD;
        }
#endif

        max_wset = std::max(max_wset, itr->get_rec_ptr()->get_tidw());
    }

    // Serialization point
    asm volatile("":: : "memory");  // NOLINT
    /**
     * In x86/64, the write-read order between different addresses is not guaranteed.
     */
    std::atomic_thread_fence(std::memory_order_release);
    ti->set_epoch(epoch::kGlobalEpoch.load(std::memory_order_acquire));
    asm volatile("":: : "memory");  // NOLINT
    /**
     * In x86/64, the order between reads (epoch read and read verify) is guaranteed.
     */

    // Phase 3: Validation
    cc_silo_variant::tid_word check;
    for (auto &&itr : ti->get_read_set()) {
        const cc_silo_variant::Record* rec_ptr = itr.get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw().get_obj());
        if ((itr.get_rec_read().get_tidw().get_epoch() != check.get_epoch() ||
             itr.get_rec_read().get_tidw().get_tid() != check.get_tid())
            ||
            check.get_absent() // check whether it was deleted.
            ||
            (check.get_lock() && (ti->search_write_set(itr.get_rec_ptr()) == nullptr))
                ) {
            ti->unlock_write_set();
            abort(token);
            return Status::ERR_VALIDATION;
        }
        max_rset = std::max(max_rset, check);
    }

    // node verify for protect phantom
#ifdef INDEX_YAKUSHIMA
    for (auto &&itr : ti->get_node_set()) {
        if (std::get<0>(itr) != std::get<1>(itr)->get_stable_version()) {
            ti->unlock_write_set();
            abort(token);
            return Status::ERR_VALIDATION;
        }
    }
#endif

    // Phase 4: Write & Unlock
    cc_silo_variant::write_phase(ti, max_rset, max_wset,
                                 cp != nullptr ? cp->get_cp() : commit_property::NOWAIT_FOR_COMMIT);
    /**
     * about holding operation info.
     */
    ti->clean_up_ops_set();
    /**
     * about scan operation.
     */
    ti->clean_up_scan_caches();

    ti->gc_records_and_values();

#if defined(PWAL)
    if (cp != nullptr) cp->set_ctid(ti->get_mrctid().get_obj());
#elif defined(CPR)
    if (cp != nullptr) {
        cpr::phase_version current_gpv = cpr::global_phase_version::get_gpv();
        if (ti->get_phase() == current_gpv.get_phase() && current_gpv.get_phase() == cpr::phase::REST) {
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

    ti->set_tx_began(false);
    return Status::OK;
}

extern bool check_commit(Token token, [[maybe_unused]]std::uint64_t commit_id) {  // NOLINT
    [[maybe_unused]] auto* ti = static_cast<cc_silo_variant::session_info*>(token);
#if defined(PWAL)
    return ti->get_flushed_ctid().get_obj() > commit_id;
#elif defined(CPR)
    return commit_id < global_phase_version::get_gpv().get_version();
#else
    /**
     * No logging method means pre-commit is commit.
     */
    return true;
#endif
}

}  // namespace shirakami::cc_silo_variant
