/**
 * @file interface_update_insert.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"
#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/interface_helper.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant {

Status insert(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              std::string_view key, std::string_view val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tx_begin(token);
  write_set_obj* inws{ti->search_write_set(key)};
  if (inws != nullptr) {
    inws->reset_tuple_value(val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

#ifdef INDEX_KOHLER_MASSTREE
  masstree_wrapper<Record>::thread_init(sched_getcpu());
  if (kohler_masstree::find_record(key.data(), key.size()) != nullptr) {
#elif INDEX_YAKUSHIMA
  if (std::get<0>(yakushima::yakushima_kvs::get<Record*>(key)) != nullptr) {
#endif
    return Status::WARN_ALREADY_EXISTS;
  }

  Record* rec_ptr =  // NOLINT
      new Record(key.data(), key.size(), val.data(), val.size());
#ifdef INDEX_KOHLER_MASSTREE
  Status insert_result(
      kohler_masstree::insert_record(key.data(), key.size(), rec_ptr));
  if (insert_result == Status::OK) {
#elif INDEX_YAKUSHIMA
  yakushima::status insert_result{
      yakushima::yakushima_kvs::put<Record*>(key, &rec_ptr)};  // NOLINT
  if (insert_result == yakushima::status::OK) {
#endif
    ti->get_write_set().emplace_back(OP_TYPE::INSERT, rec_ptr);
    return Status::OK;
  }
  delete rec_ptr;  // NOLINT
  return Status::WARN_ALREADY_EXISTS;
}

Status update(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
              std::string_view key, std::string_view val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tx_begin(token);

  write_set_obj* inws{ti->search_write_set(key)};
  if (inws != nullptr) {
    inws->reset_tuple_value(val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

#ifdef INDEX_KOHLER_MASSTREE
  masstree_wrapper<Record>::thread_init(sched_getcpu());
  Record* rec_ptr{
      kohler_masstree::get_mtdb().get_value(key.data(), key.size())};
  if (rec_ptr == nullptr) {
    return Status::WARN_NOT_FOUND;
  }
#elif INDEX_YAKUSHIMA
  Record** rec_double_ptr{
      std::get<0>(yakushima::yakushima_kvs::get<Record*>(key))};
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

  ti->get_write_set().emplace_back(key.data(), key.size(), val.data(),
                                   val.size(), OP_TYPE::UPDATE, rec_ptr);

  return Status::OK;
}

Status upsert(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              std::string_view key, std::string_view val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tx_begin(token);
  write_set_obj* in_ws{ti->search_write_set(key)};
  if (in_ws != nullptr) {
    in_ws->reset_tuple_value(val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

#ifdef INDEX_KOHLER_MASSTREE
  masstree_wrapper<Record>::thread_init(sched_getcpu());
  Record* rec_ptr{
      kohler_masstree::kohler_masstree::find_record(key.data(), key.size())};
#elif INDEX_YAKUSHIMA
  Record** rec_double_ptr{
      std::get<0>(yakushima::yakushima_kvs::get<Record*>(key))};
  Record* rec_ptr{};
  if (rec_double_ptr == nullptr) {
    rec_ptr = nullptr;
  } else {
    rec_ptr = (*std::get<0>(yakushima::yakushima_kvs::get<Record*>(key)));
  }
#endif
  if (rec_ptr == nullptr) {
    rec_ptr =  // NOLINT
        new Record(key.data(), key.size(), val.data(), val.size());
#ifdef INDEX_KOHLER_MASSTREE
    Status insert_result(
        kohler_masstree::insert_record(key.data(), key.size(), rec_ptr));
    if (insert_result == Status::OK) {
#elif INDEX_YAKUSHIMA
    yakushima::status insert_result{
        yakushima::yakushima_kvs::put<Record*>(key, &rec_ptr)};  // NOLINT
    if (insert_result == yakushima::status::OK) {
#endif
      ti->get_write_set().emplace_back(OP_TYPE::INSERT, rec_ptr);
      return Status::OK;
    }
    // else insert_result == Status::WARN_ALREADY_EXISTS
    // so goto update.
    delete rec_ptr;  // NOLINT
  }
  ti->get_write_set().emplace_back(key.data(), key.size(), val.data(),
                                   val.size(), OP_TYPE::UPDATE,
                                   rec_ptr);  // NOLINT

  return Status::OK;
}  // namespace shirakami::silo_variant

}  // namespace shirakami::cc_silo_variant
