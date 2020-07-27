/**
 * @file interface_update_insert.cpp
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

Status insert(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              const char* key, std::size_t len_key, const char* val,
              std::size_t len_val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tx_begin(token);
  WriteSetObj* inws{ti->search_write_set(key, len_key)};
  if (inws != nullptr) {
    inws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

#ifdef INDEX_KOHLER_MASSTREE
  if (kohler_masstree::find_record(key, len_key) != nullptr) {
    return Status::WARN_ALREADY_EXISTS;
  }
#endif

  Record* record =  // NOLINT
      new Record(key, len_key, val, len_val);
#ifdef INDEX_KOHLER_MASSTREE
  Status insert_result(kohler_masstree::insert_record(key, len_key, record));
#endif
  if (insert_result == Status::OK) {
    ti->get_write_set().emplace_back(OP_TYPE::INSERT, record);
    return Status::OK;
  }
  delete record;  // NOLINT
  return Status::WARN_ALREADY_EXISTS;
}

Status update(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
              const char* key, std::size_t len_key, const char* val,
              std::size_t len_val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tx_begin(token);
  masstree_wrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws{ti->search_write_set(key, len_key)};
  if (inws != nullptr) {
    inws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

#ifdef INDEX_KOHLER_MASSTREE
  Record* record{kohler_masstree::get_mtdb().get_value(key, len_key)};
#endif
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

  ti->get_write_set().emplace_back(key, len_key, val, len_val, OP_TYPE::UPDATE,
                                   record);

  return Status::OK;
}

Status upsert(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              const char* key, std::size_t len_key, const char* val,
              std::size_t len_val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tx_begin(token);
  WriteSetObj* in_ws{ti->search_write_set(key, len_key)};
  if (in_ws != nullptr) {
    in_ws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

#ifdef INDEX_KOHLER_MASSTREE
  Record* record{kohler_masstree::kohler_masstree::find_record(key, len_key)};
#endif
  if (record == nullptr) {
    record = new Record(key, len_key, val, len_val);  // NOLINT
#ifdef INDEX_KOHLER_MASSTREE
    Status insert_result(kohler_masstree::insert_record(key, len_key, record));
#endif
    if (insert_result == Status::OK) {
      ti->get_write_set().emplace_back(OP_TYPE::INSERT, record);
      return Status::OK;
    }
    // else insert_result == Status::WARN_ALREADY_EXISTS
    // so goto update.
    delete record;  // NOLINT
  }
  ti->get_write_set().emplace_back(key, len_key, val, len_val, OP_TYPE::UPDATE,
                                   record);

  return Status::OK;
}

}  // namespace shirakami::silo_variant
