/**
 * @file helper.cpp
 */

#include "cc/silo_variant//include/interface.h"

#include "cc/silo_variant/include/thread_info_table.h"
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif
#include "include/tuple_local.h"

namespace shirakami::silo_variant {

Status search_key(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                  const char* const key, const std::size_t len_key,
                  Tuple** tuple) {
  auto* ti = static_cast<silo_variant::ThreadInfo*>(token);
  if (!ti->get_txbegan()) silo_variant::tx_begin(token);

#ifdef INDEX_KOHLER_MASSTREE
  MasstreeWrapper<silo_variant::Record>::thread_init(sched_getcpu());
#endif
  silo_variant::WriteSetObj* inws{ti->search_write_set(key, len_key)};
  if (inws != nullptr) {
    if (inws->get_op() == OP_TYPE::DELETE) {
      return Status::WARN_ALREADY_DELETE;
    }
    *tuple = &inws->get_tuple(inws->get_op());
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  silo_variant::ReadSetObj* inrs{ti->search_read_set(key, len_key)};
  if (inrs != nullptr) {
    *tuple = &inrs->get_rec_read().get_tuple();
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

#ifdef INDEX_KOHLER_MASSTREE
  silo_variant::Record* record{
      index_kohler_masstree::get_mtdb().get_value(key, len_key)};
#endif
  if (record == nullptr) {
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }
  silo_variant::tid_word checktid(loadAcquire(record->get_tidw().get_obj()));
  if (checktid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }

  silo_variant::ReadSetObj rsob(record);
  Status rr = silo_variant::read_record(rsob.get_rec_read(), record);
  if (rr == Status::OK) {
    ti->get_read_set().emplace_back(std::move(rsob));
    *tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
  }
  return rr;
}

}  // namespace shirakami::silo_variant