#include "cc/silo_variant//include/interface_helper.h"
#include "cc/silo_variant/include/session_info_table.h"
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif
#include "include/tuple_local.h"

namespace shirakami::cc_silo_variant {

Status search_key(Token token, const std::string_view key,  // NOLINT
                  Tuple** const tuple) {
  auto* ti = static_cast<cc_silo_variant::session_info*>(token);
  if (!ti->get_txbegan()) cc_silo_variant::tx_begin(token);

#ifdef INDEX_KOHLER_MASSTREE
  masstree_wrapper<Record>::thread_init(sched_getcpu());
#endif
  write_set_obj* inws{ti->search_write_set(key)};
  if (inws != nullptr) {
    if (inws->get_op() == OP_TYPE::DELETE) {
      return Status::WARN_ALREADY_DELETE;
    }
    *tuple = &inws->get_tuple(inws->get_op());
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  read_set_obj* inrs{ti->search_read_set(key)};
  if (inrs != nullptr) {
    *tuple = &inrs->get_rec_read().get_tuple();
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

#ifdef INDEX_KOHLER_MASSTREE
  Record* rec_ptr{
      kohler_masstree::get_mtdb().get_value(key.data(), key.size())};
  if (rec_ptr == nullptr) {
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }
#elif INDEX_YAKUSHIMA
  Record** rec_double_ptr{
      std::get<0>(yakushima::get<Record*>(key))};
  if (rec_double_ptr == nullptr) {
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }
  Record* rec_ptr{*rec_double_ptr};
#endif
  tid_word chk_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
  if (chk_tid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }

  read_set_obj rs_ob(rec_ptr); // NOLINT
  Status rr = read_record(rs_ob.get_rec_read(), rec_ptr);
  if (rr == Status::OK) {
    ti->get_read_set().emplace_back(std::move(rs_ob));
    *tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
  }
  return rr;
}

}  // namespace shirakami::cc_silo_variant
