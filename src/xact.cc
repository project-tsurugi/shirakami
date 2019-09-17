#include "kernel.h"
#include "debug.h"
#include "tsc.hpp"
#include "xact.h"
#include "interface.h"

namespace kvs {

std::vector<LogShell> LogList;
std::vector<Record*> DataBase;
std::vector<ThreadInfo*> ThreadTable;

void lock_mutex(pthread_mutex_t *mutex);
void unlock_mutex(pthread_mutex_t *mutex);

uint64_t
loadAcquireGE()
{
  return __atomic_load_n(&(GlobalEpoch), __ATOMIC_ACQUIRE);
}

void
atomicAddGE()
{
	uint64_t expected = loadAcquireGE();
  for (;;) {
    uint64_t desired = expected + 1;
    if (__atomic_compare_exchange_n(&(GlobalEpoch), &(expected), desired, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
			break;
		}
  }
}

bool
locked_by_me(Tuple tuple, std::vector<WriteSetObj> writeSet)
{
	for (auto iws = writeSet.begin(); iws != writeSet.end(); ++iws) {
		if (iws->rec_ptr->tuple.len_key == tuple.len_key &&
				memcmp(iws->rec_ptr->tuple.key, tuple.key, tuple.len_key) == 0) {
      return true;
    }
	}

	return false;
}

static void
unlockWriteSet(std::vector<WriteSetObj> lockList)
{
	TidWord expected, desired;
  
	for (auto itr = lockList.begin(); itr != lockList.end(); ++itr) {
		Record *record = itr->rec_ptr;
		expected.obj = __atomic_load_n(&(record->tidw.obj), __ATOMIC_ACQUIRE);
		desired = expected;
		desired.lock = 0;
		__atomic_store_n(&(record->tidw.obj), desired.obj, __ATOMIC_RELEASE);
	}
}

static ThreadInfo*
get_thread_info(const uint token)
{
  ThreadInfo *ti;

  pthread_mutex_lock(&MutexThreadTable);
  for (auto itr = ThreadTable.begin(); itr != ThreadTable.end(); itr++) {
    if ((*itr)->token == token) {
      pthread_mutex_unlock(&MutexThreadTable);
      ti = *itr;
      return ti;
    }
  }
  pthread_mutex_unlock(&MutexThreadTable);

	// should not arrive here
  ERR;
}

static void 
write_phase(ThreadInfo* ti, TidWord max_rset, TidWord max_wset, std::vector<WriteSetObj> lockList)
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
	 * about readSet
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
	tid_c.epoch = ti->epoch;

	/* compare a, b, c */
	TidWord maxtid = max({tid_a, tid_b, tid_c});
	maxtid.lock = 0;
	maxtid.latest = 1;
	mrctid = maxtid;

	//DDD((int)ti->writeSet.size());
	for (auto iws = ti->writeSet.begin(); iws != ti->writeSet.end(); ++iws) {
		switch (iws->op) {
			case UPDATE:
				free(iws->rec_ptr->tuple.val);
				//DDD(iws->rec_ptr->tuple.len_val);
				//DDD(iws->update_len_val);
				if (!(iws->rec_ptr->tuple.val = (char *)calloc(iws->update_len_val, sizeof(char)))) ERR;
				memcpy(iws->rec_ptr->tuple.val, iws->update_val, iws->update_len_val);
				iws->rec_ptr->tuple.len_val = iws->update_len_val;

				break;
			case INSERT:
				iws->rec_ptr->tuple.visible = true;
				break;
			case DELETE:
				iws->rec_ptr->tuple.visible = false;
				break;
			default: ERR; break;
		}
		__atomic_store_n(&(iws->rec_ptr->tidw.obj), maxtid.obj, __ATOMIC_RELEASE);
	}

	//ERR;
  unlockWriteSet(lockList);
	lockList.clear();
	ti->readSet.clear();
	ti->writeSet.clear();
	//ERR;
}

bool
checkClockSpan(uint64_t &start, uint64_t &stop, uint64_t threshold)
{
  uint64_t diff = 0;
  diff = stop - start;
  if (diff > threshold) return true;
  else return false;
}

static bool
checkEpochLoaded(void)
{
  uint64_t curEpoch = loadAcquireGE();

  lock_mutex(&MutexThreadTable);
  for (auto itr = ThreadTable.begin(); itr != ThreadTable.end(); itr++){
		assert(*itr != NULL);
		assert(*itr != nullptr);
    if (__atomic_load_n(&(*itr)->epoch, __ATOMIC_ACQUIRE) != curEpoch) {
      unlock_mutex(&MutexThreadTable);
      return false;
    }
  }
  unlock_mutex(&MutexThreadTable);

  return true;
}

// Logging thread, not yet implemented
void *
logger(void *arg) 
{
  int fd = open(LOG_FILE, O_APPEND|O_CREAT, 0644);
  while (true) {
    uint64_t curEpoch = loadAcquireGE();
    pthread_mutex_lock(&MutexLogList);
    for (auto itr = LogList.begin(); itr != LogList.end(); itr++) {
      if (itr->epoch < curEpoch) {
        write(fd, itr->body, sizeof(LogBody) * itr->counter);
        LogList.erase(itr);
        itr--;
      }
    }
    fsync(fd);
    pthread_mutex_unlock(&MutexLogList);
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
  
	uint64_t EpochTimerStart, EpochTimerStop;

	EpochTimerStart = rdtsc();
	for (;;) {
		usleep(1);
		EpochTimerStop = rdtsc();
		// chkEpochLoaded checks whether the 
    // latest global epoch is read by all the threads
		if (checkClockSpan(EpochTimerStart, EpochTimerStop, EPOCH_TIME * CLOCK_PER_US * 1000) &&
				checkEpochLoaded()) {
			atomicAddGE();
			EpochTimerStart = EpochTimerStop;
		}
	}
	//----------

	return nullptr;
}

#ifdef WAL
static void
exec_logging(std::vector<Record> writeSet, const int myid)
{
  LogBody *lb = (LogBody *)calloc(writeSet.size(), sizeof(LogBody)); if (!lb) ERR;
  uint counter = 0;
  for (auto itr = writeSet.begin(); itr != writeSet.end(); itr++) {
    lb[counter].tidw = itr->tidw.obj;
    lb[counter].tuple = itr->tuple;
    ++counter;
  }
  LogShell ls;
  ls.epoch = ThLocalEpoch[myid];
  ls.body = lb;
  ls.counter = counter;

  pthread_mutex_lock(&MutexLogList);
  LogList.push_back(ls);
  pthread_mutex_unlock(&MutexLogList);
}
#endif

WriteSetObj
update_normal_phase(char *val, uint len_val, Record* rec_ptr)
{
  WriteSetObj wso;

  wso.update_len_val = len_val;
	if (!(wso.update_val = (char *)calloc(len_val, sizeof(char)))) ERR;
	memcpy(wso.update_val, val, len_val);
  wso.op = UPDATE;
	wso.rec_ptr = rec_ptr;
	
  return wso;
}

static WriteSetObj
insert_normal_phase(char *key, uint len_key, char *val, uint len_val)
{
  // insert temporal object
	WriteSetObj wso;
  wso.op = INSERT;

  Record* rec_ptr = new Record(key, len_key, val, len_val);
  rec_ptr->tuple.visible = false;
  lock_mutex(&MutexDB);
  DataBase.push_back(rec_ptr);
  unlock_mutex(&MutexDB);
	wso.rec_ptr = rec_ptr;
	
  return wso;
}

WriteSetObj
delete_normal_phase(char *key, const uint len_key)
{
  WriteSetObj wso;
  wso.op = DELETE;

	lock_mutex(&MutexDB);
	for (auto itr = DataBase.begin(); itr != DataBase.end(); itr++) {
		if ((*itr)->tuple.len_key == len_key && memcmp((*itr)->tuple.key, key, len_key) == 0) {
			wso.rec_ptr = *itr;
			break;
		}
	}		
	unlock_mutex(&MutexDB);
	
  return wso;
}

bool
is_locked(TidWord check)
{
  if (check.lock) return true;
  return false;
}

extern bool
kvs_commit(const int token)
{
  ThreadInfo *ti = get_thread_info(token);
  TidWord max_rset, max_wset;
  std::vector<WriteSetObj> lockList;

	//DDD((int)ti->readSet.size());
	//DDD((int)ti->writeSet.size());

  // Phase 1: Sort lock list;
  copy(ti->writeSet.begin(), ti->writeSet.end(), back_inserter(lockList));
  lockList.erase(std::unique(lockList.begin(), lockList.end()), lockList.end());
  std::sort(lockList.begin(), lockList.end());


  // Phase 2: Lock write set;
  TidWord expected, desired;
	//DDD((int)lockList.size());
  for (auto itr = lockList.begin(); itr != lockList.end(); itr++) {
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
	__atomic_store_n(&ti->epoch, loadAcquireGE(), __ATOMIC_RELEASE);
  asm volatile("" ::: "memory");

  // Phase 3: Validation
  TidWord check;
  for (auto itr = ti->readSet.begin(); itr != ti->readSet.end(); itr++) {
    // Condition 1
		Record* rec_ptr = itr->rec_ptr;
    check.obj = __atomic_load_n(&(rec_ptr->tidw.obj), __ATOMIC_ACQUIRE);
    if ((*itr).rec_read.tidw.epoch != check.epoch || (*itr).rec_read.tidw.tid != check.tid) {
      unlockWriteSet(lockList); 
      lockList.clear(); 
      ti->readSet.clear(); 
      ti->writeSet.clear();
      return false;
    }
    // Condition 3 (Cond. 2 is omitted since it is needless)
    if (is_locked(check) && (!locked_by_me((*itr).rec_read.tuple, ti->writeSet))) {
			ERR;
      unlockWriteSet(lockList); 
      lockList.clear(); 
      ti->readSet.clear(); 
      ti->writeSet.clear();
      return false;
    }
    max_rset = max(max_rset, check);
  }

  // Phase 4: Write & Unlock

  //exec_logging(writeSet, myid);

	write_phase(ti, max_rset, max_wset, lockList);

  return true;
}

static uint
get_token(void)
{
  static int token = 0;
  int latest_token;

  pthread_mutex_lock(&MutexToken);
  latest_token = token;
	token++;
	pthread_mutex_unlock(&MutexToken);
  
  return latest_token;
}

extern uint
kvs_enter(void)
{
  uint token = get_token();  
  ThreadInfo* ti = new ThreadInfo(token);
	
  //ti->token = token;
  lock_mutex(&MutexThreadTable);
  ThreadTable.push_back(ti);
	unlock_mutex(&MutexThreadTable);

  return token;
}

extern bool
kvs_leave(uint token)
{
  pthread_mutex_lock(&MutexThreadTable);
  for (auto itr = ThreadTable.begin(); itr != ThreadTable.end(); itr++) {
    if ((*itr)->token == token) {
      itr = ThreadTable.erase(itr);
			pthread_mutex_unlock(&MutexThreadTable);
			return true;
    }
  }

	return false; // failure
}

extern std::vector<Tuple*>
kvs_scan_key(uint token, char *lkey, uint len_lkey, char *rkey, uint len_rkey)
{
  ThreadInfo* ti = get_thread_info(token);
  std::vector<Tuple*> result;

  lock_mutex(&MutexDB);
	for (auto itr = DataBase.begin(); itr != DataBase.end(); itr++) {  
    if ((memcmp((*itr)->tuple.key, lkey, len_lkey) >= 0) &&
        (memcmp((*itr)->tuple.key, rkey, len_rkey) <= 0) &&
				((*itr)->tuple.visible == true)) {
			//SSS((*itr)->tuple.key);
      ReadSetObj rso;
			rso.rec_read = *(*itr);
			rso.rec_ptr = *itr;
			Tuple* tuple = new Tuple((*itr)->tuple.key, (*itr)->tuple.len_key, (*itr)->tuple.val, (*itr)->tuple.len_val);
      result.push_back(tuple);
      ti->readSet.push_back(rso);
    }
  }
  unlock_mutex(&MutexDB);

  return result;
}

extern Tuple*
kvs_search_key(uint token, char *key, uint len_key)
{
  ThreadInfo* ti = get_thread_info(token);
  Tuple *tuple = nullptr;

	//SSS(key);
	//DDD(len_key);	
  for (auto itr = DataBase.begin(); itr != DataBase.end(); itr++) {
		//SSS(itr->tuple.key);
		//DDD(itr->tuple.len_key);		
    if ((*itr)->tuple.len_key == len_key &&
				memcmp((*itr)->tuple.key, key, len_key) == 0 &&
				(*itr)->tuple.visible == true) {
		//if (itr->tuple.len_key == len_key && memcmp(itr->tuple.key, key, len_key) == 0) {
			//ERR;
      ReadSetObj rso;
			rso.rec_read = *(*itr);
			rso.rec_ptr = *itr;
			assert(rso.rec_ptr != NULL);
			assert(rso.rec_ptr != nullptr);

      ti->readSet.push_back(rso);
      tuple = new Tuple((*itr)->tuple.key, (*itr)->tuple.len_key, (*itr)->tuple.val, (*itr)->tuple.len_val);
      break;
    }
  }

  return tuple;
}

Record*
find_record(char *key, uint len_key)
{
	for (auto itr = DataBase.begin(); itr != DataBase.end(); itr++) {
		if ((*itr)->tuple.len_key == len_key &&
				memcmp((*itr)->tuple.key, key, len_key) == 0 &&
				(*itr)->tuple.visible == true) {
			return *itr;
		}
	}

	return nullptr;
}
		
extern bool
kvs_update(uint token, char *key, uint len_key, char *val, uint len_val)
{
  //Tuple tuple = make_tuple(key, len_key, val, len_val);

	//NNN;
	Record* record = find_record(key, len_key);
	if (!record) {
		//SSS(key);
		//sleep(1);
		ERR;
		return false;
	}

  WriteSetObj wso = update_normal_phase(val, len_val, record);
  ThreadInfo* ti = get_thread_info(token);
	//NNN;
  ti->writeSet.push_back(wso);
	
  return true;
}

extern bool
kvs_insert(const uint token, char *key, uint len_key, char *val, uint len_val)
{
  ThreadInfo* ti = get_thread_info(token);

	//Tuple tuple = make_tuple(key, len_key, val, len_val);
	WriteSetObj wso = insert_normal_phase(key, len_key, val, len_val);
	ti->writeSet.push_back(wso);

	/*
  for (auto itr = ThreadTable.begin(); itr != ThreadTable.end(); itr++) {
    if ((*itr)->token == token) {
			ThreadTable* ti = *itr;
      return true;
    }
  }
	*/
	
  return true;
}

extern void
kvs_delete(const uint token, char *key, uint len_key)
{
  WriteSetObj wso = delete_normal_phase(key, len_key);
  ThreadInfo* ti = get_thread_info(token);
  ti->writeSet.push_back(wso);
}

extern void
kvs_upsert(uint token, char *key, uint len_key, char *val, uint len_val)
{
  WriteSetObj wso;
  
  ThreadInfo* ti = get_thread_info(token);
  //Tuple tuple = make_tuple(key, len_key, val, len_val);
	Record* record;

	record = find_record(key, len_key);
	if (record != nullptr) {
		wso = update_normal_phase(val, len_val, record);
	}
  else {
		wso = insert_normal_phase(key, len_key, val, len_val);
	}
  ti->writeSet.push_back(wso);
}

extern void
debug_print_key(void)
{
	for (auto itr = DataBase.begin(); itr != DataBase.end(); itr++) {
		//std::cout << itr->tuple.key << ":" << itr->tuple.visible << std::endl;
		//PPP(*itr);
		;
	}
}

}  // namespace kvs
