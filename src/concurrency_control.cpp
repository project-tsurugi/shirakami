/**
 * @file concurrency_control.cpp
 */


#include "concurrency_control.h"
#include "garbage_collection.h"
#include "masstree_beta_wrapper.h"
#include "tuple_local.h"
#include "xact.h"

namespace shirakami {

void cc_silo::write_phase(ThreadInfo* ti, const tid_word& max_rset,
                        const tid_word& max_wset) {
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
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
  tid_a = std::max(max_wset, max_rset);
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
  tid_word maxtid = std::max({tid_a, tid_b, tid_c});
  maxtid.set_lock(false);
  maxtid.set_absent(false);
  maxtid.set_latest(true);
  maxtid.set_epoch(ti->get_epoch());
  ti->set_mrctid(maxtid);

#ifdef WAL
  ti->wal(maxtid.get_obj());
#endif

  for (auto iws = ti->get_write_set().begin();
       iws != ti->get_write_set().end(); ++iws) {
    Record* recptr = iws->get_rec_ptr();
    switch (iws->get_op()) {
      case OP_TYPE::UPDATE: {
        std::string* old_value{};
        std::string_view new_value_view =
            iws->get_tuple(iws->get_op()).get_value();
        recptr->get_tuple().get_pimpl()->set_value(
            new_value_view.data(), new_value_view.size(), &old_value);
        storeRelease(recptr->get_tidw().get_obj(), maxtid.get_obj());
        if (old_value != nullptr) {
          std::mutex& mutex_for_gclist =
              garbage_collection::get_mutex_garbage_values_at(
                  ti->get_gc_container_index());
          mutex_for_gclist.lock();
          ti->get_gc_value_container()->emplace_back(
              std::make_pair(old_value, ti->get_epoch()));
          mutex_for_gclist.unlock();
        }
        break;
      }
      case OP_TYPE::INSERT: {
        storeRelease(recptr->get_tidw().get_obj(), maxtid.get_obj());
        break;
      }
      case OP_TYPE::DELETE: {
        tid_word deletetid = maxtid;
        deletetid.set_absent(true);
        std::string_view key_view = recptr->get_tuple().get_key();
        MTDB.remove_value(key_view.data(), key_view.size());
        storeRelease(recptr->get_tidw().get_obj(), deletetid.get_obj());

        /**
         * create information for garbage collection.
         */
        std::mutex& mutex_for_gclist =
            garbage_collection::get_mutex_garbage_records_at(
                ti->get_gc_container_index());
        mutex_for_gclist.lock();
        ti->get_gc_record_container()->emplace_back(recptr);
        mutex_for_gclist.unlock();

        break;
      }
      default:
        std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
                  << std::endl;
        std::abort();
    }
  }

  /**
   * about holding operation info.
   */
  ti->clean_up_ops_set();
  /**
   * about scan operation.
   */
  ti->clean_up_scan_caches();

  ti->gc_records_and_values();
}
} // namespace shirakami
