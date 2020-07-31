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

#ifdef INDEX_KOHLER_MASSTREE
  std::vector<const Record*> scan_res;
  masstree_wrapper<Record>::thread_init(sched_getcpu());
  kohler_masstree::get_mtdb().scan(nullptr, 0, false, nullptr, 0, false,
                                   &scan_res, false);
#elif INDEX_YAKUSHIMA
  std::vector<std::tuple<Record**, std::size_t> > scan_res;
  yakushima::yakushima_kvs::scan("", false, "", false, scan_res);
#endif

  if (scan_res.empty()) {
    return Status::WARN_ALREADY_DELETE;
  }

  for (auto&& itr : scan_res) {
#ifdef INDEX_KOHLER_MASSTREE
    std::string_view key_view = itr->get_tuple().get_key();
#elif INDEX_YAKUSHIMA
    std::string_view key_view = (*std::get<0>(itr))->get_tuple().get_key();
#endif
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
  masstree_wrapper<Record>::thread_init(sched_getcpu());
  Record* rec_ptr{kohler_masstree::get_mtdb().get_value(key, len_key)};
  if (rec_ptr == nullptr) {
    return Status::WARN_NOT_FOUND;
  }
#elif INDEX_YAKUSHIMA
  Record** rec_double_ptr{std::get<0>(yakushima::yakushima_kvs::get<Record*>({key, len_key}))};
  if (rec_double_ptr == nullptr) {
    return Status::WARN_NOT_FOUND;
  }
  Record* rec_ptr{*rec_double_ptr};
#endif
  tid_word check_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
  if (check_tid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    return Status::WARN_NOT_FOUND;
  }

  ti->get_write_set().emplace_back(OP_TYPE::DELETE, rec_ptr);
  return check;
}

}  // namespace shirakami::silo_variant
