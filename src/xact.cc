
#include "include/cache_line_size.hh"
#include "include/masstree_wrapper.hh"
#include "include/mutex.hh"
#include "include/scheme.h"
#include "include/tsc.hh"
#include "include/xact.hh"

#include "kvs/debug.h"
#include "kvs/interface.h"

namespace kvs {

std::vector<LogShell> LogList;
alignas(CACHE_LINE_SIZE) MasstreeWrapper<Record> MTDB;

/* This variable has informations about worker thread. */
std::vector<ThreadInfo*> kThreadTable;
__thread ThreadInfo* kTI = nullptr;

void lock_mutex(pthread_mutex_t *mutex);
void unlock_mutex(pthread_mutex_t *mutex);

extern void
delete_database()
{
}

uint64_t
load_acquire_ge()
{
  return __atomic_load_n(&(kGlobalEpoch), __ATOMIC_ACQUIRE);
}

void
atomic_add_global_epoch()
{
  uint64_t expected = load_acquire_ge();
  for (;;) {
    uint64_t desired = expected + 1;
    if (__atomic_compare_exchange_n(&(kGlobalEpoch), &(expected), desired, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
      break;
    }
  }
}

bool
locked_by_me(Tuple tuple, std::vector<WriteSetObj>& write_set)
{
  for (auto iws = write_set.begin(); iws != write_set.end(); ++iws) {
    if (iws->rec_ptr->tuple.len_key == tuple.len_key &&
        memcmp(iws->rec_ptr->tuple.key.get(), tuple.key.get(), tuple.len_key) == 0) {
      return true;
    }
  }

  return false;
}

static void
unlock_write_set(std::vector<WriteSetObj>& write_set)
{
  TidWord expected, desired;
  
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).op == DELETE) continue;
    Record *record = itr->rec_ptr;
    expected.obj = __atomic_load_n(&(record->tidw.obj), __ATOMIC_ACQUIRE);
    desired = expected;
    desired.lock = 0;
    __atomic_store_n(&(record->tidw.obj), desired.obj, __ATOMIC_RELEASE);
  }
}

static ThreadInfo*
get_thread_info(const Token token)
{
  ThreadInfo *ti;

  pthread_mutex_lock(&kMutexThreadTable);
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); itr++) {
    if ((*itr)->token == token) {
      pthread_mutex_unlock(&kMutexThreadTable);
      ti = *itr;
      return ti;
    }
  }
  pthread_mutex_unlock(&kMutexThreadTable);

  // should not arrive here
  ERR;
}

static void 
write_phase(TidWord max_rset, TidWord max_wset)
{
  /*
   * It calculates the smallest number that is 
   * (a) larger than the TID of any record read or written by the transaction,
   * (b) larger than the worker's most recently chosen TID,
   * and (C) in the current global epoch.
   */
  TidWord tid_a, tid_b, tid_c;
  TidWord mrctid;

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
  tid_b = mrctid;
  tid_b.tid++;

  /* calculates (c) */
  tid_c.epoch = kTI->epoch;

  /* compare a, b, c */
  TidWord maxtid = max({tid_a, tid_b, tid_c});
  maxtid.lock = 0;
  maxtid.latest = 1;
  mrctid = maxtid;

  for (auto iws = kTI->write_set.begin(); iws != kTI->write_set.end(); ++iws) {
    switch (iws->op) {
      case UPDATE:
        iws->rec_ptr->tuple.val.reset();
        iws->rec_ptr->tuple.val = std::move(iws->update_val_ptr);
        iws->rec_ptr->tuple.len_val = iws->update_len_val;

        break;
      case INSERT:
        iws->rec_ptr->tuple.visible = true;
        break;
      case DELETE:
        iws->rec_ptr->tuple.visible = false;
        MTDB.remove_value(iws->rec_ptr->tuple.key.get());
        delete iws->rec_ptr;
        break;
      default: ERR; break;
    }
    if (iws->op != DELETE)
      __atomic_store_n(&(iws->rec_ptr->tidw.obj), maxtid.obj, __ATOMIC_RELEASE);
  }

  unlock_write_set(kTI->write_set);
  kTI->read_set.clear();
  kTI->write_set.clear();
}

bool
check_clock_span(uint64_t &start, uint64_t &stop, uint64_t threshold)
{
  uint64_t diff = 0;
  diff = stop - start;
  if (diff > threshold) return true;
  else return false;
}

static bool
check_epoch_loaded(void)
{
  uint64_t curEpoch = load_acquire_ge();

  lock_mutex(&kMutexThreadTable);
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr){
    if (__atomic_load_n(&(*itr)->epoch, __ATOMIC_ACQUIRE) != curEpoch) {
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
    for (auto itr = LogList.begin(); itr != LogList.end(); itr++) {
      if (itr->epoch < curEpoch) {
        write(fd, itr->body, sizeof(LogBody) * itr->counter);
        LogList.erase(itr);
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
  
  uint64_t start, stop;

  start = rdtsc();
  for (;;) {
    usleep(1);
    stop = rdtsc();
    // chkEpochLoaded checks whether the 
    // latest global epoch is read by all the threads
    if (check_clock_span(start, stop, EPOCH_TIME * CLOCK_PER_US * 1000) &&
        check_epoch_loaded()) {
      atomic_add_global_epoch();
      start = stop;
    }
  }
  //----------

  return nullptr;
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
  LogList.push_back(ls);
  pthread_mutex_unlock(&kMutexLogList);
}
#endif

static void
insert_normal_phase(char const *key, std::size_t len_key, char const *val, std::size_t len_val, WriteSetObj& wso)
{
  Record* rec_ptr = new Record(key, len_key, val, len_val);
  MTDB.insert_value(key, rec_ptr);
  wso.rec_ptr = rec_ptr;
  wso.update_len_val = len_val;
  wso.op = INSERT;
}

bool
is_locked(TidWord check)
{
  if (check.lock) return true;
  return false;
}

extern Status
commit(Token token)
{
  TidWord max_rset, max_wset;

  // Phase 1: Sort lock list;
  std::sort(kTI->write_set.begin(), kTI->write_set.end());


  // Phase 2: Lock write set;
  TidWord expected, desired;
  for (auto itr = kTI->write_set.begin(); itr != kTI->write_set.end(); ++itr) {
    //Record *record = itr->rec_ptr;
    expected.obj = __atomic_load_n(&(itr->rec_ptr->tidw.obj), __ATOMIC_ACQUIRE);
    for (;;) {
      if (expected.lock) {
        expected.obj = __atomic_load_n(&(itr->rec_ptr->tidw.obj), __ATOMIC_ACQUIRE);
      } else {
        desired = expected;
        desired.lock = 1;
        if (__atomic_compare_exchange_n(&(itr->rec_ptr->tidw.obj), &(expected.obj), desired.obj, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) break;
      }
    }
  }


  // Serialization point
  asm volatile("" ::: "memory");
  __atomic_store_n(&kTI->epoch, load_acquire_ge(), __ATOMIC_RELEASE);
  asm volatile("" ::: "memory");

  // Phase 3: Validation
  TidWord check;
  for (auto itr = kTI->read_set.begin(); itr != kTI->read_set.end(); itr++) {
    // Condition 1
    Record* rec_ptr = itr->rec_ptr;
    check.obj = __atomic_load_n(&(rec_ptr->tidw.obj), __ATOMIC_ACQUIRE);
    if ((*itr).rec_read.tidw.epoch != check.epoch || (*itr).rec_read.tidw.tid != check.tid) {
      unlock_write_set(kTI->write_set); 
      kTI->read_set.clear(); 
      kTI->write_set.clear();
      return Status::ERR_VALIDATION;
    }
    // Condition 3 (Cond. 2 is omitted since it is needless)
    if (is_locked(check) && (!locked_by_me((*itr).rec_read.tuple, kTI->write_set))) {
      unlock_write_set(kTI->write_set); 
      kTI->read_set.clear(); 
      kTI->write_set.clear();
      return Status::ERR_VALIDATION;
    }
    max_rset = max(max_rset, check);
  }

  // Phase 4: Write & Unlock

  //exec_logging(write_set, myid);

  write_phase(max_rset, max_wset);

  return Status::OK;
}

/**
 * @brief Check wheter the session is already started. This function is not thread safe. But this function can be used only after taking mutex.
 */
static Status
chck_session_started(const Token token)
{
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    if ((*itr)->token == token) return Status::WARN_ALREADY_IN_A_SESSION;
  }

  return Status::OK;
}

extern Status
enter(Token& token)
{
  ThreadInfo* ti = new ThreadInfo(token);
  //printf("enter: gen threadinfo %p\n", ti);
  kTI = ti;
  
  lock_mutex(&kMutexThreadTable);
  Status chk_status = chck_session_started(token);

  if (chk_status == Status::OK) {
    kThreadTable.emplace_back(ti);
    unlock_mutex(&kMutexThreadTable);
    MasstreeWrapper<Record>::thread_init(token);
  } else if (chk_status == Status::WARN_ALREADY_IN_A_SESSION) {
    unlock_mutex(&kMutexThreadTable);
  } else {
    ERR;
  }

  return chk_status;
}

extern Status
leave(Token token)
{
  pthread_mutex_lock(&kMutexThreadTable);
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    if ((*itr)->token == token) {
      ThreadInfo *del_target = (*itr);
      kThreadTable.erase(itr);
      delete del_target;
      pthread_mutex_unlock(&kMutexThreadTable);
      kTI = nullptr;
      return Status::OK;
    }
  }

  return Status::WARN_NOT_IN_A_SESSION;
}

extern Status
scan_key(Token token, Storage storage,
    char const *lkey, std::size_t len_lkey, bool l_exclusive,
    char const *rkey, std::size_t len_rkey, bool r_exclusive,
    std::vector<Tuple*>& result)
{
  /*
  lock_mutex(&kMutexDB);
  for (auto itr = DataBase.begin(); itr != DataBase.end(); itr++) {  
    if ((memcmp((*itr)->tuple.key, lkey, len_lkey) >= 0) &&
        (memcmp((*itr)->tuple.key, rkey, len_rkey) <= 0) &&
        ((*itr)->tuple.visible == true)) {
      Tuple* tuple = new Tuple((*itr)->tuple.key, (*itr)->tuple.len_key, (*itr)->tuple.val, (*itr)->tuple.len_val);
      result.push_back(tuple);
      ti->read_set.push_back(ReadSetObj(*itr));
    }
  }
  unlock_mutex(&kMutexDB);
  */

  return Status::OK;
}

extern Status
search_key(Token token, Storage storage, char const *key, std::size_t len_key, Tuple** tuple)
{
  WriteSetObj* inws = kTI->search_write_set(key, len_key);
  if (inws != nullptr) return Status::OK;
  ReadSetObj* inrs = kTI->search_read_set(key, len_key);
  if (inrs != nullptr) return Status::OK;

  Record* record = MTDB.get_value(key);
  always_assert(record, "keys must exist");
  kTI->read_set.emplace_back(ReadSetObj(record));
  *tuple = &record->tuple;

  return Status::OK;
}

extern Status
update(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val)
{
  WriteSetObj* inws = kTI->search_write_set(key, len_key, UPDATE);
  if (inws != nullptr) return Status::OK;

  Record* record = MTDB.get_value(key);
  if (!record) {
    return Status::ERR_NOT_FOUND;
  }

  WriteSetObj wso(val, len_val, UPDATE, record);
  kTI->write_set.emplace_back(std::move(wso));
  
  return Status::OK;
}


extern Status
insert(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val)
{
  //printf("insert: threadinfo %p\n", kTI);
  WriteSetObj* inws = kTI->search_write_set(key, len_key, INSERT);
  if (inws != nullptr) return Status::OK;
  if (MTDB.get_value(key) != nullptr) {
    NNN;
    return Status::ERR_ALREADY_EXISTS;
  }

  WriteSetObj wso;
  insert_normal_phase(key, len_key, val, len_val, wso);
  kTI->write_set.emplace_back(std::move(wso));
  return Status::OK;
}

extern Status
delete_record(Token token, Storage storage, char const *key, std::size_t len_key)
{
  WriteSetObj wso(DELETE, MTDB.get_value(key));
  kTI->write_set.emplace_back(std::move(wso));

  return Status::OK;
}

extern Status
upsert(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val)
{
  Record* record = MTDB.get_value(key);
  WriteSetObj wso;
  
  if (record != nullptr) {
    wso.reset(val, len_val, UPDATE, record);
  }
  else {
    insert_normal_phase(key, len_key, val, len_val, wso);
  }
  kTI->write_set.emplace_back(std::move(wso));

  return Status::OK;
}

extern void
print_MTDB(void)
{
  // Future work.
  // MTDB.print_table();
}

} //  namespace kvs
