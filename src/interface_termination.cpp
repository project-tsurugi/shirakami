/**
 * @file interface_termination.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"
#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/helper.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

Status abort(Token token) {  // NOLINT
  auto* ti = static_cast<silo_variant::ThreadInfo*>(token);
  ti->remove_inserted_records_of_write_set_from_masstree();
  ti->clean_up_ops_set();
  ti->clean_up_scan_caches();
  ti->set_tx_began(false);
  ti->gc_records_and_values();
  return Status::OK;
}

Status commit(Token token) {  // NOLINT
  auto* ti = static_cast<silo_variant::ThreadInfo*>(token);
  silo_variant::tid_word max_rset;
  silo_variant::tid_word max_wset;

  // Phase 1: Sort lock list;
  std::sort(ti->get_write_set().begin(), ti->get_write_set().end());

  // Phase 2: Lock write set;
  silo_variant::tid_word expected;
  silo_variant::tid_word desired;
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
  asm volatile("" ::: "memory");  // NOLINT
  ti->set_epoch(silo_variant::epoch::load_acquire_global_epoch());
  asm volatile("" ::: "memory");  // NOLINT

  // Phase 3: Validation
  silo_variant::tid_word check;
  for (auto itr = ti->get_read_set().begin(); itr != ti->get_read_set().end();
       itr++) {
    const silo_variant::Record* rec_ptr = itr->get_rec_ptr();
    check.get_obj() = loadAcquire(rec_ptr->get_tidw().get_obj());
    if ((itr->get_rec_read().get_tidw().get_epoch() != check.get_epoch() ||
         itr->get_rec_read().get_tidw().get_tid() != check.get_tid()) ||
        check.get_absent()  // check whether it was deleted.
        || (check.get_lock() &&
            (ti->search_write_set(itr->get_rec_ptr()) == nullptr))) {
      ti->unlock_write_set();
      abort(token);
      return Status::ERR_VALIDATION;
    }
    max_rset = std::max(max_rset, check);
  }

  // Phase 4: Write & Unlock

  // exec_logging(write_set, myid);

  silo_variant::write_phase(ti, max_rset, max_wset);

  ti->set_tx_began(false);
  return Status::OK;
}

}  //  namespace shirakami
