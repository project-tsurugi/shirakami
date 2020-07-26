/**
 * @file interface_delete.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"
#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/interface.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami::silo_variant {

[[maybe_unused]] Status delete_all_records() {  // NOLINT

  Token s{};
  Storage st{};
  while (Status::OK != enter(s)) _mm_pause();

  std::vector<const Record*> scan_res;
#ifdef INDEX_KOHLER_MASSTREE
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  index_kohler_masstree::get_mtdb().scan(nullptr, 0, false, nullptr, 0, false,
                                         &scan_res, false);
#endif  // INDEX_KOHLER_MASSTREE

  if (scan_res.empty()) {
    return Status::WARN_ALREADY_DELETE;
  }

  for (auto&& itr : scan_res) {
    std::string_view key_view = itr->get_tuple().get_key();
    delete_record(s, st, key_view.data(), key_view.size());
    Status result = commit(s);
    if (result != Status::OK) return result;
  }

  leave(s);
  return Status::OK;
}

Status delete_record(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                     const char* key, std::size_t len_key) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tx_begin(token);
  Status check = ti->check_delete_after_write(key, len_key);

#ifdef INDEX_KOHLER_MASSTREE
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  Record* record{index_kohler_masstree::get_mtdb().get_value(key, len_key)};
#endif  // INDEX_KOHLER_MASSTREE
  if (record == nullptr) {
    return Status::WARN_NOT_FOUND;
  }
  tid_word check_tid(loadAcquire(record->get_tidw().get_obj()));
  if (check_tid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    return Status::WARN_NOT_FOUND;
  }

  ti->get_write_set().emplace_back(OP_TYPE::DELETE, record);
  return check;
}

}  // namespace shirakami::silo_variant
