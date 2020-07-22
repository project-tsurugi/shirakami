/**
 * @file xact.cc
 * @brief implement about transaction
 */

#include "xact.h"

#include <bitset>

#include "cache_line_size.h"
#include "cpu.h"
#include "garbage_collection.h"
#include "masstree_beta_wrapper.h"
#include "scheme_local.h"
#include "tuple_local.h"

namespace shirakami {

alignas(CACHE_LINE_SIZE) MasstreeWrapper<Record> MTDB;              // NOLINT

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

Record* find_record_from_masstree(char const* key,  // NOLINT
                                  std::size_t len_key) {
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return MTDB.get_value(key, len_key);
}

}  //  namespace shirakami
