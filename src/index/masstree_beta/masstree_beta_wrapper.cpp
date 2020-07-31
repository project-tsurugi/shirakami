/**
 * @file masstree_wrapper.cc
 * @brief implement about masstree_wrapper
 */

#include "index/masstree_beta/include/masstree_beta_wrapper.h"

#include <bitset>

#include "cpu.h"
#include "include/tuple_local.h"

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
[[maybe_unused]] volatile bool recovering = false;

namespace shirakami {

Status kohler_masstree::insert_record(char const* key,  // NOLINT
                                            std::size_t len_key,
                                      cc_silo_variant::Record* record) {
#ifdef INDEX_KOHLER_MASSTREE
#ifdef KVS_Linux
  int core_pos = sched_getcpu();
  if (core_pos == -1) {
    std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
              << std::endl;
    std::abort();
  }
  cpu_set_t current_mask = getThreadAffinity();
  setThreadAffinity(core_pos);
#endif  // KVS_Linux
  masstree_wrapper<cc_silo_variant::Record>::thread_init(sched_getcpu());
  Status insert_result(MTDB.insert_value(key, len_key, record));
#ifdef KVS_Linux
  setThreadAffinity(current_mask);
#endif  // KVS_Linux
  return insert_result;
#endif  // INDEX_KOHLER_MASSTREE
}

cc_silo_variant::Record* kohler_masstree::find_record(
    char const* key,  // NOLINT
    std::size_t len_key) {
#ifdef INDEX_KOHLER_MASSTREE
  masstree_wrapper<cc_silo_variant::Record>::thread_init(sched_getcpu());
  return MTDB.get_value(key, len_key);
#endif  // INDEX_KOHLER_MASSTREE
}

}  // namespace shirakami
