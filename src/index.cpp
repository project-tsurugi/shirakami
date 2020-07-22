/**
 * @file xact.cc
 * @brief implement about transaction
 */

#include "index.h"

#include <bitset>

#include "cpu.h"
#include "garbage_collection.h"
#include "scheme_local.h"
#include "tuple_local.h"

// index choice
#include "masstree_beta_wrapper.h"

namespace shirakami {

Status index_kohler_masstree::insert_record(
    char const* key,  // NOLINT
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

Record* index_kohler_masstree::find_record(
    char const* key,  // NOLINT
    std::size_t len_key) {
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return MTDB.get_value(key, len_key);
}

}  //  namespace shirakami
