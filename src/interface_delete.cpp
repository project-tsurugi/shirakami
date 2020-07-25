/**
 * @file interface_delete.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"
#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/helper.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

[[maybe_unused]] Status delete_all_records() {  // NOLINT
  Token s{};
  Storage st{};
  while (Status::OK != enter(s)) _mm_pause();
  MasstreeWrapper<silo_variant::Record>::thread_init(sched_getcpu());

  std::vector<const silo_variant::Record*> scan_res;
  index_kohler_masstree::get_mtdb().scan(nullptr, 0, false, nullptr, 0, false,
                                         &scan_res, false);

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

Status delete_record(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
                     const char* const key, const std::size_t len_key) {
  auto* ti = static_cast<silo_variant::ThreadInfo*>(token);
  if (!ti->get_txbegan()) silo_variant::tbegin(token);
  Status check = ti->check_delete_after_write(key, len_key);

  MasstreeWrapper<silo_variant::Record>::thread_init(sched_getcpu());

  silo_variant::Record* record{
      index_kohler_masstree::get_mtdb().get_value(key, len_key)};
  if (record == nullptr) {
    return Status::WARN_NOT_FOUND;
  }
  silo_variant::tid_word check_tid(loadAcquire(record->get_tidw().get_obj()));
  if (check_tid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    return Status::WARN_NOT_FOUND;
  }

  ti->get_write_set().emplace_back(OP_TYPE::DELETE, record);
  return check;
}

}  //  namespace shirakami
