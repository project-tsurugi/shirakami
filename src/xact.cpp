/**
 * @file xact.cc
 * @brief implement about transaction
 */

#include "xact.h"

#include <bitset>

#include "atomic_wrapper.h"
#include "cache_line_size.h"
#include "cpu.h"
#include "epoch.h"
#include "garbage_collection.h"
#include "masstree_beta_wrapper.h"
#include "scheme_local.h"
#include "tuple_local.h"

namespace shirakami {

alignas(CACHE_LINE_SIZE)
    std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS> kThreadTable;  // NOLINT
alignas(CACHE_LINE_SIZE) MasstreeWrapper<Record> MTDB;              // NOLINT

void tbegin(Token token) {
  auto* ti = static_cast<ThreadInfo*>(token);
  ti->set_txbegan(true);
  ti->set_epoch(epoch::load_acquire_global_epoch());
}

Status insert_record_to_masstree(char const* key,  // NOLINT
                                 std::size_t len_key, Record* record) {
#ifdef KVS_Linux
  int core_pos = sched_getcpu();
  if (core_pos == -1) {
    std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
              << std::endl;
    std::abort();
  }
  cpu_set_t current_mask = getThreadAffinity();
  setThreadAffinity(core_pos);
#endif
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  Status insert_result(MTDB.insert_value(key, len_key, record));
#ifdef KVS_Linux
  setThreadAffinity(current_mask);
#endif
  return insert_result;
}

Status read_record(Record& res, const Record* const dest) {  // NOLINT
  tid_word f_check;
  tid_word s_check;  // first_check, second_check for occ

  f_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));

  for (;;) {
    while (f_check.get_lock()) {
      f_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));
    }

    if (f_check.get_absent()) {
      return Status::WARN_CONCURRENT_DELETE;
      // other thread is inserting this record concurrently,
      // but it is't committed yet.
    }

    res.get_tuple() = dest->get_tuple();  // execute copy assign.

    s_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));
    if (f_check == s_check) {
      break;
    }
    f_check = s_check;
  }

  res.set_tidw(f_check);
  return Status::OK;
}

Record* find_record_from_masstree(char const* key,  // NOLINT
                                  std::size_t len_key) {
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return MTDB.get_value(key, len_key);
}

}  //  namespace shirakami
