/**
 * @file xact.cc
 * @brief implement about transaction
 */

#include "xact.hh"

#include <bitset>

#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "cpu.hh"
#include "epoch.hh"
#include "gcollection.hh"
#include "masstree_wrapper.hh"
#include "scheme.hh"
#include "tuple.hh"

namespace kvs {

alignas(CACHE_LINE_SIZE)
    std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS> kThreadTable;  // NOLINT
alignas(CACHE_LINE_SIZE) MasstreeWrapper<Record> MTDB;              // NOLINT

void tbegin(Token token) {
  auto* ti = static_cast<ThreadInfo*>(token);
  ti->set_txbegan(true);
  ti->set_epoch(load_acquire_ge());
}

static void write_phase(ThreadInfo* ti, const TidWord& max_rset,
                        const TidWord& max_wset) {
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  /*
   * It calculates the smallest number that is
   * (a) larger than the TID of any record read or written by the transaction,
   * (b) larger than the worker's most recently chosen TID,
   * and (C) in the current global epoch.
   */
  TidWord tid_a;
  TidWord tid_b;
  TidWord tid_c;

  /*
   * calculates (a)
   * about read_set
   */
  tid_a = std::max(max_wset, max_rset);
  tid_a.inc_tid();

  /*
   * calculates (b)
   * larger than the worker's most recently chosen TID,
   */
  tid_b = ti->get_mrctid();
  tid_b.inc_tid();

  /* calculates (c) */
  tid_c.set_epoch(ti->get_epoch());

  /* compare a, b, c */
  TidWord maxtid = std::max({tid_a, tid_b, tid_c});
  maxtid.set_lock(false);
  maxtid.set_absent(false);
  maxtid.set_latest(true);
  maxtid.set_epoch(ti->get_epoch());
  ti->set_mrctid(maxtid);

#ifdef WAL
  ti->wal(maxtid.get_obj());
#endif

  for (auto iws = ti->get_write_set().begin(); iws != ti->get_write_set().end(); ++iws) {
    Record* recptr = iws->get_rec_ptr();
    switch (iws->get_op()) {
      case OP_TYPE::UPDATE: {
        std::string* old_value{};
        std::string_view new_value_view =
            iws->get_tuple(iws->get_op()).get_value();
        recptr->get_tuple().get_pimpl()->set_value(
            new_value_view.data(), new_value_view.size(), &old_value);
        storeRelease(recptr->get_tidw().get_obj(), maxtid.get_obj());
        if (old_value != nullptr) {
          std::mutex& mutex_for_gclist =
              kMutexGarbageValues.at(ti->get_gc_container_index());
          mutex_for_gclist.lock();
          ti->get_gc_value_container()->emplace_back(
              std::make_pair(old_value, ti->get_epoch()));
          mutex_for_gclist.unlock();
        }
        break;
      }
      case OP_TYPE::INSERT: {
        storeRelease(recptr->get_tidw().get_obj(), maxtid.get_obj());
        break;
      }
      case OP_TYPE::DELETE: {
        TidWord deletetid = maxtid;
        deletetid.set_absent(true);
        std::string_view key_view = recptr->get_tuple().get_key();
        MTDB.remove_value(key_view.data(), key_view.size());
        storeRelease(recptr->get_tidw().get_obj(), deletetid.get_obj());

        /**
         * create information for garbage collection.
         */
        std::mutex& mutex_for_gclist =
            kMutexGarbageRecords.at(ti->get_gc_container_index());
        mutex_for_gclist.lock();
        ti->get_gc_record_container()->emplace_back(recptr);
        mutex_for_gclist.unlock();

        break;
      }
      default:
        std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
                  << std::endl;
        std::abort();
    }
  }

  /**
   * about holding operation info.
   */
  ti->clean_up_ops_set();
  /**
   * about scan operation.
   */
  ti->clean_up_scan_caches();

  ti->gc_records_and_values();
}

Status abort(Token token) {  // NOLINT
  auto* ti = static_cast<ThreadInfo*>(token);
  ti->remove_inserted_records_of_write_set_from_masstree();
  ti->clean_up_ops_set();
  ti->clean_up_scan_caches();
  ti->set_txbegan(false);
  ti->gc_records_and_values();
  return Status::OK;
}

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

Status commit(Token token) {  // NOLINT
  auto* ti = static_cast<ThreadInfo*>(token);
  TidWord max_rset;
  TidWord max_wset;

  // Phase 1: Sort lock list;
  std::sort(ti->get_write_set().begin(), ti->get_write_set().end());

  // Phase 2: Lock write set;
  TidWord expected;
  TidWord desired;
  for (auto itr = ti->get_write_set().begin(); itr != ti->get_write_set().end(); ++itr) {
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
  ti->set_epoch(load_acquire_ge());
  asm volatile("" ::: "memory");  // NOLINT

  // Phase 3: Validation
  TidWord check;
  for (auto itr = ti->get_read_set().begin(); itr != ti->get_read_set().end(); itr++) {
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

  write_phase(ti, max_rset, max_wset);

  ti->set_txbegan(false);
  return Status::OK;
}

/**
 * @brief Check wheter the session is already started. This function is not
 * thread safe. But this function can be used only after taking mutex.
 */
static Status decide_token(Token& token) {  // NOLINT
  for (auto&& itr : kThreadTable) {
    if (!itr.get_visible()) {
      bool expected(false);
      bool desired(true);
      if (itr.cas_visible(expected, desired)) {
        token = static_cast<void*>(&(itr));
        break;
      }
    }
    if (&itr == kThreadTable.end() - 1) return Status::ERR_SESSION_LIMIT;
  }

  return Status::OK;
}

Status enter(Token& token) {  // NOLINT
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return decide_token(token);
}

Status leave(Token token) {  // NOLINT
  for (auto&& itr : kThreadTable) {
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

Status read_record(Record& res, const Record* const dest) {  // NOLINT
  TidWord f_check;
  TidWord s_check;  // first_check, second_check for occ

  f_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));

  for (;;) {
    while (f_check.get_lock()) {
      f_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));
    }

    if (f_check.get_absent()) {
      return Status::WARN_CONCURRENT_DELETE;
      // other thread is inserting this record concurrently,
      // but it is't committed yet.
    }

    res.get_tuple() = dest->get_tuple();  // execute copy assign.

    s_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));
    if (f_check == s_check) {
      break;
    }
    f_check = s_check;
  }

  res.set_tidw(f_check);
  return Status::OK;
}

[[maybe_unused]] Status delete_all_records() {  // NOLINT
  Token s{};
  Storage st{};
  while (Status::OK != enter(s)) _mm_pause();
  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  std::vector<const Record*> scan_res;
  MTDB.scan(nullptr, 0, false, nullptr, 0, false, &scan_res, false);

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

Status search_key(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
                  const char* const key, const std::size_t len_key,
                  Tuple** const tuple) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    if (inws->get_op() == OP_TYPE::DELETE) {
      return Status::WARN_ALREADY_DELETE;
    }
    *tuple = &inws->get_tuple(inws->get_op());
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  ReadSetObj* inrs = ti->search_read_set(key, len_key);
  if (inrs != nullptr) {
    *tuple = &inrs->get_rec_read().get_tuple();
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  Record* record = MTDB.get_value(key, len_key);
  if (record == nullptr) {
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }
  TidWord checktid(loadAcquire(record->get_tidw().get_obj()));
  if (checktid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }

  ReadSetObj rsob(record);
  Status rr = read_record(rsob.get_rec_read(), record);
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
  if (!ti->get_txbegan()) tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    inws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  Record* record = MTDB.get_value(key, len_key);
  if (record == nullptr) {
    return Status::WARN_NOT_FOUND;
  }
  TidWord checktid(loadAcquire(record->get_tidw().get_obj()));
  if (checktid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    return Status::WARN_NOT_FOUND;
  }

  ti->get_write_set().emplace_back(key, len_key, val, len_val, OP_TYPE::UPDATE,
                             record);

  return Status::OK;
}

Status insert(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
              const char* const key, const std::size_t len_key,
              const char* const val, const std::size_t len_val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tbegin(token);
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    inws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  if (find_record_from_masstree(key, len_key) != nullptr) {
    return Status::WARN_ALREADY_EXISTS;
  }

  Record* record = new Record(key, len_key, val, len_val);  // NOLINT
  Status insert_result(insert_record_to_masstree(key, len_key, record));
  if (insert_result == Status::OK) {
    ti->get_write_set().emplace_back(OP_TYPE::INSERT, record);
    return Status::OK;
  }
  // else insert_result == Status::WARN_ALREADY_EXISTS
  delete record;  // NOLINT
  return Status::WARN_ALREADY_EXISTS;
}

Status delete_record(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
                     const char* const key, const std::size_t len_key) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tbegin(token);
  Status check = ti->check_delete_after_write(key, len_key);

  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  Record* record = MTDB.get_value(key, len_key);
  if (record == nullptr) {
    return Status::WARN_NOT_FOUND;
  }
  TidWord checktid(loadAcquire(record->get_tidw().get_obj()));
  if (checktid.get_absent()) {
    // The second condition checks
    // whether the record you want to read should not be read by parallel
    // insert / delete.
    return Status::WARN_NOT_FOUND;
  }

  ti->get_write_set().emplace_back(OP_TYPE::DELETE, record);
  return check;
}

Record* find_record_from_masstree(char const* key,  // NOLINT
                                  std::size_t len_key) {
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return MTDB.get_value(key, len_key);
}

Status upsert(Token token, [[maybe_unused]] Storage storage, // NOLINT
              const char* const key, std::size_t len_key,
              const char* const val, std::size_t len_val) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tbegin(token);
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    inws->reset_tuple_value(val, len_val);
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  Record* record = find_record_from_masstree(key, len_key);
  if (record == nullptr) {
    record = new Record(key, len_key, val, len_val); // NOLINT
    Status insert_result(insert_record_to_masstree(key, len_key, record));
    if (insert_result == Status::OK) {
      ti->get_write_set().emplace_back(OP_TYPE::INSERT, record);
      return Status::OK;
    }
    // else insert_result == Status::WARN_ALREADY_EXISTS
    // so goto update.
    delete record; // NOLINT
  }
  ti->get_write_set().emplace_back(key, len_key, val, len_val, OP_TYPE::UPDATE,
                             record);

  return Status::OK;
}

}  //  namespace kvs
