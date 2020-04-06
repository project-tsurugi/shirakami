
/**
 * @file xact.cc
 * @brief implement about transaction 
 */

#include "xact.hh"

#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "clock.hh"
#include "cpu.hh"
#include "debug.hh"
#include "epoch.hh"
#include "gcollection.hh"
#include "masstree_wrapper.hh"
#include "key.hh"
#include "kvs.hh"
#include "mutex.hh"
#include "scheme.hh"
#include "tsc.hh"
#include "kvs/interface.h"

// for output debug. finally, it should delete these.
using std::cout;
using std::endl;

namespace kvs {

alignas(CACHE_LINE_SIZE) std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS> kThreadTable;
alignas(CACHE_LINE_SIZE) MasstreeWrapper<Record> MTDB;

void
delete_all_garbage_records()
{
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto itr = kGarbageRecords[i].begin(); itr != kGarbageRecords[i].end(); ++itr) {
      delete *itr;
    }
    kGarbageRecords[i].clear();
  }
}

void
tbegin(Token token)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  ti->txbegan_ = true;
  __atomic_store_n(&ti ->epoch, load_acquire_ge(), __ATOMIC_RELEASE);
}

static void 
write_phase(ThreadInfo* ti, TidWord max_rset, TidWord max_wset)
{
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  /*
   * It calculates the smallest number that is 
   * (a) larger than the TID of any record read or written by the transaction,
   * (b) larger than the worker's most recently chosen TID,
   * and (C) in the current global epoch.
   */
  TidWord tid_a, tid_b, tid_c;

  /* 
   * calculates (a) 
   * about read_set
   */
  tid_a = max(max_wset, max_rset);
  tid_a.tid++;
  
  /*
   * calculates (b)
   * larger than the worker's most recently chosen TID,
   */
  tid_b = ti->mrctid;
  tid_b.tid++;

  /* calculates (c) */
  tid_c.epoch = ti->epoch;

  /* compare a, b, c */
  TidWord maxtid = max({tid_a, tid_b, tid_c});
  maxtid.lock = 0;
  maxtid.absent = 0;
  maxtid.latest = 1;
  maxtid.epoch = ti->epoch;
  ti->mrctid = maxtid;

#ifdef WAL
  ti->wal(maxtid.obj);
#endif

  for (auto iws = ti->write_set.begin(); iws != ti->write_set.end(); ++iws) {
    switch (iws->op) {
      case OP_TYPE::UPDATE:
        iws->rec_ptr->tuple.val.reset();
        iws->rec_ptr->tuple.val = std::move(iws->tuple.val);
        iws->rec_ptr->tuple.len_val = iws->tuple.len_val;
        __atomic_store_n(&(iws->rec_ptr->tidw.obj), maxtid.obj, __ATOMIC_RELEASE);
        break;
      case OP_TYPE::INSERT:
        __atomic_store_n(&(iws->rec_ptr->tidw.obj), maxtid.obj, __ATOMIC_RELEASE);
        break;
      case OP_TYPE::DELETE:
        {
          TidWord deletetid = maxtid;
          deletetid.absent = 1;
          MTDB.remove_value(iws->rec_ptr->tuple.key.get(), iws->rec_ptr->tuple.len_key);

          /**
           * create information for garbage collection.
           */
          std::mutex& mutex_for_gclist = kMutexGarbageRecords[ti->gc_container_index_];
          mutex_for_gclist.lock();
          ti->gc_container_->emplace_back(iws->rec_ptr);
          mutex_for_gclist.unlock();

          __atomic_store_n(&(iws->rec_ptr->tidw.obj), deletetid.obj, __ATOMIC_RELEASE);
          break;
        }
      default:
        ERR;
        break;
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

  gc_records();
}

Status
abort(Token token)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  ti->remove_inserted_records_of_write_set_from_masstree();
  ti->clean_up_ops_set();
  ti->clean_up_scan_caches();
  ti->txbegan_ = false;
  gc_records();
  return Status::OK;
}

void
forced_gc_all_records()
{
  for (uint i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    auto itr = kGarbageRecords[i].begin();
    while (itr != kGarbageRecords[i].end()) {
      delete *itr;
      itr = kGarbageRecords[i].erase(itr);
    }
  }
}

static void
insert_record_to_masstree(char const *key, std::size_t len_key, Record* record)
{
#ifdef KVS_Linux
  int core_pos = sched_getcpu();
  if (core_pos == -1) ERR;
  cpu_set_t current_mask = getThreadAffinity();
  setThreadAffinity(core_pos);
#endif
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  MTDB.insert_value(key, len_key, record);
#ifdef KVS_Linux
  setThreadAffinity(current_mask);
#endif
}

Status
commit(Token token)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  TidWord max_rset, max_wset;

  // Phase 1: Sort lock list;
  std::sort(ti->write_set.begin(), ti->write_set.end());


  // Phase 2: Lock write set;
  TidWord expected, desired;
  for (auto itr = ti->write_set.begin(); itr != ti->write_set.end(); ++itr) {
    if (itr->op == OP_TYPE::INSERT) continue;
    expected.obj = loadAcquire(itr->rec_ptr->tidw.obj);
    for (;;) {
      if (expected.lock) {
        expected.obj = loadAcquire(itr->rec_ptr->tidw.obj);
      } else {
        desired = expected;
        desired.lock = 1;
        if (__atomic_compare_exchange_n(&(itr->rec_ptr->tidw.obj), &(expected.obj), desired.obj, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) break;
      }
    }
    if (itr->op == OP_TYPE::UPDATE && itr->rec_ptr->tidw.absent == true) {
      ti->unlock_write_set(ti->write_set.begin(), itr);
      abort(token);
      return Status::ERR_WRITE_TO_DELETED_RECORD;
    }

    max_wset = max(max_wset, expected);
  }


  // Serialization point
  asm volatile("" ::: "memory");
  storeRelease(ti->epoch, load_acquire_ge());
  asm volatile("" ::: "memory");

  // Phase 3: Validation
  TidWord check;
  for (auto itr = ti->read_set.begin(); itr != ti->read_set.end(); itr++) {
    Record* rec_ptr = itr->rec_ptr;
    check.obj = loadAcquire(rec_ptr->tidw.obj);
    if (((*itr).rec_read.tidw.epoch != check.epoch || (*itr).rec_read.tidw.tid != check.tid)
        || (check.absent == 1) // check whether it was deleted.
        || (check.lock && (ti->search_write_set((*itr).rec_ptr) == nullptr))) {
      ti->unlock_write_set();
      abort(token);
      return Status::ERR_VALIDATION;
    }
    max_rset = max(max_rset, check);
  }

  // Phase 4: Write & Unlock

  //exec_logging(write_set, myid);

  write_phase(ti, max_rset, max_wset);

  ti->txbegan_ = false;
  return Status::OK;
}

/**
 * @brief Check wheter the session is already started. This function is not thread safe. But this function can be used only after taking mutex.
 */
static Status
decide_token(Token& token)
{
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    if (itr->visible.load(std::memory_order_acquire) == false) {
      bool expected(false);
      bool desired(true);
      if (itr->visible.compare_exchange_strong(expected, desired, std::memory_order_acq_rel)) {
        token = static_cast<void*>(&(*itr));
        break;
      }
    }
    if (itr == kThreadTable.end() - 1) return Status::ERR_SESSION_LIMIT;
  }

  return Status::OK;
}

Status
enter(Token& token)
{
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return decide_token(token);
}

Status
leave(Token token)
{
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    if (&(*itr) == static_cast<ThreadInfo*>(token)) {
      if (itr->visible.load(std::memory_order_acquire) == true) {
        itr->visible.store(false, std::memory_order_release);
        return Status::OK;
      } else {
        return Status::WARN_NOT_IN_A_SESSION;
      }
    }
  }
  return Status::ERR_INVALID_ARGS;
}

Status
read_record(Record& res, Record* dest)
{
  TidWord f_check, s_check; // first_check, second_check for occ

  f_check.obj = loadAcquire(dest->tidw.obj);

  for (;;) {
    while (f_check.lock)
      f_check.obj = loadAcquire(dest->tidw.obj);

    if (f_check.absent == true) {
      return Status::WARN_CONCURRENT_DELETE;
      // other thread is inserting this record concurrently,
      // but it is't committed yet.
    }


    res.tuple = dest->tuple; // execute copy assign.

    s_check.obj = loadAcquire(dest->tidw.obj);
    if (f_check == s_check) break;
    f_check = s_check;
  }

  res.tidw = f_check;
  return Status::OK;
}

Status
delete_all_records()
{
  Token s{};
  Storage st{};
  while (Status::OK != enter(s)) _mm_pause();
  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  std::vector<Record*> scan_res;
  MTDB.scan(nullptr, 0, false, nullptr, 0, false, &scan_res);

  if (scan_res.size() == 0) {
    return Status::WARN_ALREADY_DELETE;
  }

  for (auto itr = scan_res.begin(); itr != scan_res.end(); ++itr) {
    delete_record(s, st, (*itr)->tuple.key.get(), (*itr)->tuple.len_key);
    Status result = commit(s);
    if (result != Status::OK) return result;
  }

  leave(s);
  return Status::OK;
}

Status
search_key(Token token, Storage storage, const char* const key, const std::size_t len_key, Tuple** const tuple)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  if (!ti->txbegan_) tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    if (inws->op == OP_TYPE::DELETE) {
      return Status::WARN_ALREADY_DELETE;
    }
    *tuple = &inws->tuple;
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  ReadSetObj* inrs = ti->search_read_set(key, len_key);
  if (inrs != nullptr) {
    *tuple = &inrs->rec_read.tuple;
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  Record* record = MTDB.get_value(key, len_key);
  if (unlikely(record == nullptr)) {
    *tuple = nullptr;
    return Status::WARN_NOT_FOUND;
  }

  ReadSetObj rsob(record);
  Status rr = read_record(rsob.rec_read, record);
  if (rr == Status::OK) {
    ti->read_set.emplace_back(std::move(rsob));
    *tuple = &ti->read_set.back().rec_read.tuple;
  }
  return rr;
}

Status
update(Token token, Storage storage, const char* const key, const std::size_t len_key, const char* const val, const std::size_t len_val)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  if (!ti->txbegan_) tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    inws->reset(val, len_val); 
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  Record* record = MTDB.get_value(key, len_key);
  if (unlikely(record == nullptr)) {
    return Status::WARN_NOT_FOUND;
  }

  WriteSetObj wso(key, len_key, val, len_val, OP_TYPE::UPDATE, record);
  ti->write_set.emplace_back(std::move(wso));
  
  return Status::OK;
}


Status
insert(Token token, Storage storage, const char* const key, const std::size_t len_key, const char* const val, const std::size_t len_val)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  if (!ti->txbegan_) tbegin(token);
  WriteSetObj* inws = ti->search_write_set(key, len_key, OP_TYPE::INSERT);
  if (inws != nullptr) {
    inws->reset(val, len_val); 
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  if (find_record_from_masstree(key, len_key) != nullptr) {
    return Status::WARN_ALREADY_EXISTS;
  }

  Record* record = new Record(key, len_key, val, len_val);
  insert_record_to_masstree(key, len_key, record);
  ti->write_set.emplace_back(key, len_key, val, len_val, OP_TYPE::INSERT, record);
  return Status::OK;
}

Status
delete_record(Token token, Storage storage, const char* const key, const std::size_t len_key)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  if (!ti->txbegan_) tbegin(token);
  Status check = ti->check_delete_after_write(key, len_key);

  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  Record* record = MTDB.get_value(key, len_key);
  if (record == nullptr) {
    return Status::WARN_NOT_FOUND;
  };

  ti->write_set.emplace_back(key, len_key, OP_TYPE::DELETE, record);
  return check;
}

static Record*
find_record_from_masstree(char const *key, std::size_t len_key)
{
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return MTDB.get_value(key, len_key);
}

Status
upsert(Token token, Storage storage, const char* const key, const std::size_t len_key, const char* const val, const std::size_t len_val)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  if (!ti->txbegan_) tbegin(token);
  Record *record = find_record_from_masstree(key, len_key);
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    inws->reset(val, len_val); 
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  if (record == nullptr) {
    record = new Record(key, len_key, val, len_val);
    insert_record_to_masstree(key, len_key, record);
    ti->write_set.emplace_back(key, len_key, val, len_val, OP_TYPE::INSERT, record);
  }
  else {
    ti->write_set.emplace_back(key, len_key, val, len_val, OP_TYPE::UPDATE, record);
  }

  return Status::OK;
}

} //  namespace kvs
