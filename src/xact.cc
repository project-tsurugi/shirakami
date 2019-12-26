
/**
 * @file
 * @brief impl around transaction engine interface
 */

#include "include/atomic_wrapper.hh"
#include "include/cache_line_size.hh"
#include "include/clock.hh"
#include "include/cpu.hh"
#include "include/debug.hh"
#include "include/epoch.hh"
#include "include/masstree_wrapper.hh"
#include "include/mutex.hh"
#include "include/scheme.h"
#include "include/tsc.hh"
#include "include/xact.hh"

#include "kvs/interface.h"

// for output debug. finally, it should delete these.
using std::cout;
using std::endl;

namespace kvs {

alignas(CACHE_LINE_SIZE) std::vector<LogShell> kLogList;
alignas(CACHE_LINE_SIZE) std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS> kThreadTable;
/* kGarbageRecords is a list of garbage records.
 * Theoretically, each worker thread has own list.
 * But in this kvs, the position of core at which worker is may change.
 * This is problem. It prepare enough list for experiments as pending solution.*/
alignas(CACHE_LINE_SIZE) std::vector<Record*> kGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::mutex kMutexGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) MasstreeWrapper<Record> MTDB;
/* This variable has informations about worker thread. */

extern void
delete_database()
{
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto itr = kGarbageRecords[i].begin(); itr != kGarbageRecords[i].end(); ++itr) {
      delete *itr;
    }
    kGarbageRecords[i].clear();
  }
}

extern void
tbegin(Token token)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  __atomic_store_n(&ti ->epoch, load_acquire_ge(), __ATOMIC_RELEASE);
}

static void
unlock_write_set(std::vector<WriteSetObj>& write_set)
{
  TidWord expected, desired;
  
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    Record *record = itr->rec_ptr;
    expected.obj = __atomic_load_n(&(record->tidw.obj), __ATOMIC_ACQUIRE);
    desired = expected;
    desired.lock = 0;
    __atomic_store_n(&(record->tidw.obj), desired.obj, __ATOMIC_RELEASE);
  }
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

  for (auto iws = ti->write_set.begin(); iws != ti->write_set.end(); ++iws) {
    switch (iws->op) {
      case OP_TYPE::UPDATE:
        iws->rec_ptr->tuple.val.reset();
        iws->rec_ptr->tuple.val = std::move(iws->update_val_ptr);
        iws->rec_ptr->tuple.len_val = iws->update_len_val;
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
#ifdef KVS_Linux
          int core_pos = sched_getcpu();
          if (core_pos == -1) ERR;
          cpu_set_t current_mask = getThreadAffinity();
          setThreadAffinity(core_pos);
#endif
          std::mutex& mutex_for_gclist = kMutexGarbageRecords[core_pos];
          mutex_for_gclist.lock();
          kGarbageRecords[core_pos].emplace_back(iws->rec_ptr);
          mutex_for_gclist.unlock();
#ifdef KVS_Linux
          setThreadAffinity(current_mask);
#endif
          __atomic_store_n(&(iws->rec_ptr->tidw.obj), deletetid.obj, __ATOMIC_RELEASE);
          break;
        }
      default:
        ERR;
        break;
    }
  }

  ti->read_set.clear();
  ti->write_set.clear();
  ti->notify_local_write.clear();
  gc_records();
}

extern Status
abort(Token token)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  ti->read_set.clear();
  ti->write_set.clear();
  ti->notify_local_write.clear();
  gc_records();
  return Status::OK;
}

static bool
check_epoch_loaded(void)
{
  uint64_t curEpoch = load_acquire_ge();

  lock_mutex(&kMutexThreadTable);
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr){
    if (loadAcquire(itr->epoch) != curEpoch) {
      unlock_mutex(&kMutexThreadTable);
      return false;
    }
  }
  unlock_mutex(&kMutexThreadTable);

  return true;
}

// Logging thread, not yet implemented
void *
logger(void *arg) 
{
  int fd = open(LOG_FILE, O_APPEND|O_CREAT, 0644);
  while (true) {
    uint64_t curEpoch = load_acquire_ge();
    pthread_mutex_lock(&kMutexLogList);
    for (auto itr = kLogList.begin(); itr != kLogList.end(); itr++) {
      if (itr->epoch < curEpoch) {
        write(fd, itr->body, sizeof(LogBody) * itr->counter);
        kLogList.erase(itr);
        itr--;
      }
    }
    fsync(fd);
    pthread_mutex_unlock(&kMutexLogList);
    usleep(10);
  }
}

// Epoch thread
void *
epocher(void *arg) 
{
  // Increment global epoch in each 40ms.
  // To increment it, 
  // all the worker-threads need to read the latest one.
 
#ifdef KVS_Linux
  setThreadAffinity(static_cast<int>(CorePosition::EPOCHER));
#endif

  for (;;) {
    sleepMs(KVS_EPOCH_TIME);

    // check_epoch_loaded() checks whether the 
    // latest global epoch is read by all the threads
    while (!check_epoch_loaded()) { 
      _mm_pause(); 
    }

    atomic_add_global_epoch();
    storeRelease(kReclamationEpoch, loadAcquire(kGlobalEpoch) - 2);
  }

  return nullptr;
}

extern void
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
gc_records()
{
#ifdef KVS_Linux
  int core_pos = sched_getcpu();
  if (core_pos == -1) ERR;
  cpu_set_t current_mask = getThreadAffinity();
  setThreadAffinity(core_pos);
#endif
  std::mutex& mutex_for_gclist = kMutexGarbageRecords[core_pos];
  if (mutex_for_gclist.try_lock()) {
    auto itr = kGarbageRecords[core_pos].begin();
    while (itr != kGarbageRecords[core_pos].end()) {
      if ((*itr)->tidw.epoch <= loadAcquire(kReclamationEpoch)) {
        delete *itr;
        itr = kGarbageRecords[core_pos].erase(itr);
      } else {
        break;
      }
    }
    mutex_for_gclist.unlock();
  }
#ifdef KVS_Linux
  setThreadAffinity(current_mask);
#endif
}

#ifdef WAL
static void
exec_logging(std::vector<Record> write_set, const int myid)
{
  LogBody *lb = (LogBody *)calloc(write_set.size(), sizeof(LogBody)); if (!lb) ERR;
  uint counter = 0;
  for (auto itr = write_set.begin(); itr != write_set.end(); itr++) {
    lb[counter].tidw = itr->tidw.obj;
    lb[counter].tuple = itr->tuple;
    ++counter;
  }
  LogShell ls;
  ls.epoch = ThLocalEpoch[myid];
  ls.body = lb;
  ls.counter = counter;

  pthread_mutex_lock(&kMutexLogList);
  kLogList.push_back(ls);
  pthread_mutex_unlock(&kMutexLogList);
}
#endif

static void
insert_record_to_masstree(char const *key, std::size_t len_key, char const *val, std::size_t len_val, Record** record)
{
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  Record* rec_ptr = new Record(key, len_key, val, len_val);
  MTDB.insert_value(key, len_key, rec_ptr);
  *record = rec_ptr;
}

extern Status
commit(Token token)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  TidWord max_rset, max_wset;

  // Phase 1: Sort lock list;
  std::sort(ti->write_set.begin(), ti->write_set.end());


  // Phase 2: Lock write set;
  TidWord expected, desired;
  for (auto itr = ti->write_set.begin(); itr != ti->write_set.end(); ++itr) {
    //Record *record = itr->rec_ptr;
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
      unlock_write_set(ti->write_set);
      abort(token);
      return Status::ERR_VALIDATION;
    }
    max_rset = max(max_rset, check);
  }

  // Phase 4: Write & Unlock

  //exec_logging(write_set, myid);

  write_phase(ti, max_rset, max_wset);

  return Status::OK;
}

/**
 * @brief Check wheter the session is already started. This function is not thread safe. But this function can be used only after taking mutex.
 */
static Status
decide_token(Token& token)
{
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    if (itr->visible.load(std::memory_order_acquire) == true) {
      continue;
    } else {
      bool expected(false);
      bool desired(true);
      if (itr->visible.compare_exchange_strong(expected, desired, std::memory_order_acq_rel)) {
        token = static_cast<void*>(&(*itr));
        break;
      } else {
        continue;
      }
    }
  }
  return Status::OK;
}

extern Status
enter(Token& token)
{
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  decide_token(token);
  return Status::OK;
}

extern Status
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

static Status
read_record(Record& res, Record* dest)
{
  TidWord f_check, s_check; // first_check, second_check for occ

  f_check.obj = loadAcquire(dest->tidw.obj);
  if (f_check.absent == true) {
    return Status::ERR_ILLEGAL_STATE;
    // other thread is inserting this record concurrently,
    // but it is't committed yet.
  }

  for (;;) {
    while (f_check.lock)
      f_check.obj = loadAcquire(dest->tidw.obj);

    if (f_check.absent == true) {
      return Status::ERR_ILLEGAL_STATE;
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

extern Status
scan_key(Token token, Storage storage,
    char const *lkey, std::size_t len_lkey, bool l_exclusive,
    char const *rkey, std::size_t len_rkey, bool r_exclusive,
    std::vector<Tuple*>& result)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  // as a precaution
  result.clear();

  std::vector<Record*> scan_res;
  MTDB.scan(lkey, len_lkey, l_exclusive, rkey, len_rkey, r_exclusive, &scan_res);

  //cout << std::string((*scan_res.begin())->tuple.key.get(), (*scan_res.begin())->tuple.len_key) << endl;
  for (auto itr = scan_res.begin(); itr != scan_res.end(); ++itr) {
    WriteSetObj* inws = ti->search_write_set(*itr);
    if (inws != nullptr) {
      result.emplace_back(&(*itr)->tuple);
      continue;
    }
    ReadSetObj* inrs = ti->search_read_set(*itr);
    if (inrs != nullptr) {
      result.emplace_back(&(*itr)->tuple);
      continue;
    }
    // if the record was already read/update/insert in the same transaction, 
    // the result which is record pointer is notified to caller but
    // don't execute re-read (read_record function).
    // Because in herbrand semantics, the read reads last update even if the update is own.

    ReadSetObj rsob(*itr);
    if (Status::OK != read_record(rsob.rec_read, *itr)) {
      abort(token);
      return Status::ERR_ILLEGAL_STATE;
    }
    ti->read_set.emplace_back(std::move(rsob));
    result.emplace_back(&(*itr)->tuple);
  }

  return Status::OK;
}

extern Status
search_key(Token token, Storage storage, char const *key, std::size_t len_key, Tuple** tuple)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    if (inws->op == OP_TYPE::DELETE) {
      return Status::WARN_ALREADY_DELETE;
    }
    ti->notify_local_write.push_back(Tuple(key, len_key, inws->update_val_ptr.get(), inws->update_len_val));
    *tuple = &ti->notify_local_write.back();
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
    abort(token);
    return Status::ERR_NOT_FOUND;
  }

  ReadSetObj rsob(record);
  if (Status::OK != read_record(rsob.rec_read, record)) {
    abort(token);
    return Status::ERR_ILLEGAL_STATE;
  }
  ti->read_set.emplace_back(std::move(rsob));
  *tuple = &ti->read_set.back().rec_read.tuple;

  return Status::OK;
}

extern Status
update(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    inws->reset(val, len_val); 
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  Record* record = MTDB.get_value(key, len_key);
  if (unlikely(record == nullptr)) {
    abort(token);
    return Status::ERR_NOT_FOUND;
  }

  WriteSetObj wso(val, len_val, OP_TYPE::UPDATE, record);
  ti->write_set.emplace_back(std::move(wso));
  
  return Status::OK;
}


extern Status
insert(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  WriteSetObj* inws = ti->search_write_set(key, len_key, OP_TYPE::INSERT);
  if (inws != nullptr) {
    inws->reset(val, len_val); 
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  if (find_record_from_masstree(key, len_key) != nullptr) {
    abort(token);
    return Status::ERR_ALREADY_EXISTS;
  }

  Record *record;
  insert_record_to_masstree(key, len_key, val, len_val, &record);
  ti->write_set.emplace_back(val, len_val, OP_TYPE::INSERT, record);
  return Status::OK;
}

extern Status
delete_record(Token token, Storage storage, char const *key, std::size_t len_key)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  Status check = ti->check_delete_after_upsert(key, len_key);

  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  Record* record = MTDB.get_value(key, len_key);
  if (record == nullptr) {
    abort(token);
    return Status::ERR_NOT_FOUND;
  };

  WriteSetObj wso(OP_TYPE::DELETE, record);
  ti->write_set.emplace_back(std::move(wso));

  return check;
}

static Record*
find_record_from_masstree(char const *key, std::size_t len_key)
{
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  return MTDB.get_value(key, len_key);
}

extern Status
upsert(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  Record *record = find_record_from_masstree(key, len_key);
  WriteSetObj* inws = ti->search_write_set(key, len_key);
  if (inws != nullptr) {
    inws->reset(val, len_val); 
    return Status::WARN_WRITE_TO_LOCAL_WRITE;
  }

  if (record == nullptr) {
    insert_record_to_masstree(key, len_key, val, len_val, &record);
    ti->write_set.emplace_back(val, len_val, OP_TYPE::INSERT, record);
  }
  else {
    ti->write_set.emplace_back(val, len_val, OP_TYPE::UPDATE, record);
  }

  return Status::OK;
}

extern void
print_MTDB(void)
{
  // Future work.
  // MTDB.print_table();
}

} //  namespace kvs
