/**
 * @file interface.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"
#include "boost/filesystem.hpp"
#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/silo_variant.h"
#include "cc/silo_variant/include/thread_info_table.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

Status abort(Token token) {  // NOLINT
  auto* ti = static_cast<ThreadInfo*>(token);
  ti->remove_inserted_records_of_write_set_from_masstree();
  ti->clean_up_ops_set();
  ti->clean_up_scan_caches();
  ti->set_tx_began(false);
  ti->gc_records_and_values();
  return Status::OK;
}

Status commit(Token token) {  // NOLINT
  auto* ti = static_cast<ThreadInfo*>(token);
  tid_word max_rset;
  tid_word max_wset;

  // Phase 1: Sort lock list;
  std::sort(ti->get_write_set().begin(), ti->get_write_set().end());

  // Phase 2: Lock write set;
  tid_word expected;
  tid_word desired;
  for (auto itr = ti->get_write_set().begin(); itr != ti->get_write_set().end();
       ++itr) {
    if (itr->get_op() == OP_TYPE::INSERT) continue;
    // after this, update/delete
    expected.get_obj() = loadAcquire(itr->get_rec_ptr()->get_tidw().get_obj());
    for (;;) {
      if (expected.get_lock()) {
        expected.get_obj() =
            loadAcquire(itr->get_rec_ptr()->get_tidw().get_obj());
      } else {
        desired = expected;
        desired.set_lock(true);
        if (compareExchange(itr->get_rec_ptr()->get_tidw().get_obj(),
                            expected.get_obj(), desired.get_obj())) {
          break;
        }
      }
    }
    if (itr->get_op() == OP_TYPE::UPDATE &&  // NOLINT
        itr->get_rec_ptr()->get_tidw().get_absent()) {
      ti->unlock_write_set(ti->get_write_set().begin(), itr);
      abort(token);
      return Status::ERR_WRITE_TO_DELETED_RECORD;
    }

    max_wset = std::max(max_wset, expected);
  }

  // Serialization point
  asm volatile("" ::: "memory");  // NOLINT
  ti->set_epoch(epoch::load_acquire_global_epoch());
  asm volatile("" ::: "memory");  // NOLINT

  // Phase 3: Validation
  tid_word check;
  for (auto itr = ti->get_read_set().begin(); itr != ti->get_read_set().end();
       itr++) {
    const Record* rec_ptr = itr->get_rec_ptr();
    check.get_obj() = loadAcquire(rec_ptr->get_tidw().get_obj());
    if ((itr->get_rec_read().get_tidw().get_epoch() != check.get_epoch() ||
         itr->get_rec_read().get_tidw().get_tid() != check.get_tid()) ||
        check.get_absent()  // check whether it was deleted.
        || (check.get_lock() &&
            (ti->search_write_set(itr->get_rec_ptr()) == nullptr))) {
      ti->unlock_write_set();
      abort(token);
      return Status::ERR_VALIDATION;
    }
    max_rset = std::max(max_rset, check);
  }

  // Phase 4: Write & Unlock

  // exec_logging(write_set, myid);

  cc_silo::write_phase(ti, max_rset, max_wset);

  ti->set_tx_began(false);
  return Status::OK;
}

[[maybe_unused]] Status delete_all_records() {  // NOLINT
  Token s{};
  Storage st{};
  while (Status::OK != enter(s)) _mm_pause();
  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  std::vector<const Record*> scan_res;
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
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) cc_silo::tbegin(token);
  Status check = ti->check_delete_after_write(key, len_key);

  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  Record* record{index_kohler_masstree::get_mtdb().get_value(key, len_key)};
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

Status enter(Token& token) {  // NOLINT
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return thread_info_table::decide_token(token);
}

void fin() {
  garbage_collection::release_all_heap_objects();

  // Stop DB operation.
  epoch::set_epoch_thread_end(true);
  epoch::join_epoch_thread();
  thread_info_table::fin_kThreadTable();
}

Status init(std::string_view log_directory_path) {  // NOLINT
  /**
   * The default value of log_directory is PROJECT_ROOT.
   */
  Log::set_kLogDirectory(log_directory_path);
  if (log_directory_path == MAC2STR(PROJECT_ROOT)) {
    Log::get_kLogDirectory().append("/log");
  }

  /**
   * check whether log_directory_path is filesystem objects.
   */
  boost::filesystem::path log_dir{Log::get_kLogDirectory()};
  if (boost::filesystem::exists(log_dir)) {
    /**
     * some file exists.
     * check whether it is directory.
     */
    if (!boost::filesystem::is_directory(log_dir)) {
      return Status::ERR_INVALID_ARGS;
    }
  } else {
    /**
     * directory which has log_directory_path as a file path doesn't exist.
     * it can create.
     */
    boost::filesystem::create_directories(log_dir);
  }

  /**
   * If it already exists log files, it recoveries from those.
   */
  // single_recovery_from_log();

  thread_info_table::init_kThreadTable();
  epoch::invoke_epocher();

  return Status::OK;
}

Status insert(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
              const char* const key, const std::size_t len_key,
              const char* const val, const std::size_t len_val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) cc_silo::tbegin(token);
  WriteSetObj* inws{ti->search_write_set(key, len_key)};
  if (inws != nullptr) {
    inws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  if (index_kohler_masstree::find_record(key, len_key) != nullptr) {
    return Status::WARN_ALREADY_EXISTS;
  }

  Record* record = new Record(key, len_key, val, len_val);  // NOLINT
  Status insert_result(
      index_kohler_masstree::insert_record(key, len_key, record));
  if (insert_result == Status::OK) {
    ti->get_write_set().emplace_back(OP_TYPE::INSERT, record);
    return Status::OK;
  }
  // else insert_result == Status::WARN_ALREADY_EXISTS
  delete record;  // NOLINT
  return Status::WARN_ALREADY_EXISTS;
}

Status leave(Token token) {  // NOLINT
  for (auto&& itr : thread_info_table::get_thread_info_table()) {
    if (&itr == static_cast<ThreadInfo*>(token)) {
      if (itr.get_visible()) {
        itr.set_visible(false);
        return Status::OK;
      }
      return Status::WARN_NOT_IN_A_SESSION;
    }
  }
  return Status::ERR_INVALID_ARGS;
}

Status search_key(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
                  const char* const key, const std::size_t len_key,
                  Tuple** const tuple) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) cc_silo::tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws{ti->search_write_set(key, len_key)};
  if (inws != nullptr) {
    if (inws->get_op() == OP_TYPE::DELETE) {
      return Status::WARN_ALREADY_DELETE;
    }
    *tuple = &inws->get_tuple(inws->get_op());
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  ReadSetObj* inrs{ti->search_read_set(key, len_key)};
  if (inrs != nullptr) {
    *tuple = &inrs->get_rec_read().get_tuple();
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  Record* record{index_kohler_masstree::get_mtdb().get_value(key, len_key)};
  if (record == nullptr) {
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }
  tid_word checktid(loadAcquire(record->get_tidw().get_obj()));
  if (checktid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }

  ReadSetObj rsob(record);
  Status rr = cc_silo::read_record(rsob.get_rec_read(), record);
  if (rr == Status::OK) {
    ti->get_read_set().emplace_back(std::move(rsob));
    *tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
  }
  return rr;
}

Status update(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
              const char* const key, const std::size_t len_key,
              const char* const val, const std::size_t len_val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) cc_silo::tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws{ti->search_write_set(key, len_key)};
  if (inws != nullptr) {
    inws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  Record* record{index_kohler_masstree::get_mtdb().get_value(key, len_key)};
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
              const char* const key, std::size_t len_key, const char* const val,
              std::size_t len_val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) cc_silo::tbegin(token);
  WriteSetObj* in_ws{ti->search_write_set(key, len_key)};
  if (in_ws != nullptr) {
    in_ws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  Record* record{
      index_kohler_masstree::index_kohler_masstree::find_record(key, len_key)};
  if (record == nullptr) {
    record = new Record(key, len_key, val, len_val);  // NOLINT
    Status insert_result(
        index_kohler_masstree::insert_record(key, len_key, record));
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

}  //  namespace shirakami
