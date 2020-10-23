#include <bitset>

#include "atomic_wrapper.h"

#ifdef CC_SILO_VARIANT

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

Status commit(Token token) {  // NOLINT
    auto* ti = static_cast<cc_silo_variant::session_info*>(token);
    cc_silo_variant::tid_word max_rset;
    cc_silo_variant::tid_word max_wset;

    // Phase 1: Sort lock list;
    std::sort(ti->get_write_set().begin(), ti->get_write_set().end());

    // Phase 2: Lock write set;
    cc_silo_variant::tid_word expected;
    cc_silo_variant::tid_word desired;
    for (auto itr = ti->get_write_set().begin(); itr != ti->get_write_set().end();
         ++itr) {
        if (itr->get_op() == OP_TYPE::INSERT) continue;
        // after this, update/delete
        expected.get_obj() = loadAcquire(itr->get_rec_ptr()->get_tidw().get_obj());
        for (;;) {
            if (expected.get_lock()) {
                expected.get_obj() =
                        loadAcquire(itr->get_rec_ptr()->get_tidw().get_obj());
            } else {
                desired = expected;
                desired.set_lock(true);
                if (compareExchange(itr->get_rec_ptr()->get_tidw().get_obj(),
                                    expected.get_obj(), desired.get_obj())) {
                    break;
                }
            }
        }
        if (itr->get_op() == OP_TYPE::UPDATE &&  // NOLINT
            itr->get_rec_ptr()->get_tidw().get_absent()) {
            ti->unlock_write_set(ti->get_write_set().begin(), itr);
            abort(token);
            return Status::ERR_WRITE_TO_DELETED_RECORD;
        }

        max_wset = std::max(max_wset, expected);
    }

    // Serialization point
    asm volatile("":: : "memory");  // NOLINT
    ti->set_epoch(cc_silo_variant::epoch::load_acquire_global_epoch());
    asm volatile("":: : "memory");  // NOLINT

    // Phase 3: Validation
    cc_silo_variant::tid_word check;
    for (auto itr = ti->get_read_set().begin(); itr != ti->get_read_set().end();
         itr++) {
        const cc_silo_variant::Record* rec_ptr = itr->get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw().get_obj());
        if ((itr->get_rec_read().get_tidw().get_epoch() != check.get_epoch() ||
             itr->get_rec_read().get_tidw().get_tid() != check.get_tid()) ||
            check.get_absent() ||  // check whether it was deleted.
            (check.get_lock() &&
             (ti->search_write_set(itr->get_rec_ptr()) == nullptr))
            #ifdef INDEX_YAKUSHIMA
            // phantom protection
            ||
            (itr->get_is_scan() &&
             (itr->get_nv().first != itr->get_nv().second->get_stable_version()))) {
#elif INDEX_KOHLER_MASSTREE
            ) {
#endif
            ti->unlock_write_set();
            abort(token);
            return Status::ERR_VALIDATION;
        }
        max_rset = std::max(max_rset, check);
    }

    // Phase 4: Write & Unlock
    cc_silo_variant::write_phase(ti, max_rset, max_wset);

    ti->set_tx_began(false);
    return Status::OK;
}

}  // namespace shirakami::cc_silo_variant
