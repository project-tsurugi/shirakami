/**
 * @file body of implementation about scheme
 */

#include "include/atomic_wrapper.hh"
#include "include/log.hh"
#include "include/scheme.hh"
#include "include/xact.hh"

using std::cout;
using std::endl;

namespace kvs{

void ThreadInfo::clean_up_ops_set()
{
  read_set.clear();
  write_set.clear();
  opr_set.clear();
}

void ThreadInfo::clean_up_scan_caches()
{
  scan_cache_.clear();
  scan_cache_itr_.clear();
  rkey_.clear();
  len_rkey_.clear();
  r_exclusive_.clear();
}
 
Status ThreadInfo::check_delete_after_write(const char* key, const std::size_t len_key)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).rec_ptr->tuple.len_key == len_key
        && memcmp((*itr).rec_ptr->tuple.key.get(), key, len_key) == 0) {
      write_set.erase(itr);
      return Status::WARN_CANCEL_PREVIOUS_OPERATION;
    }
  }

  return Status::OK;
}

void ThreadInfo::display_write_set()
{
  cout << "ThreadInfo::display_write_set() : write set size " 
    << write_set.size() << endl;
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    (*itr).display();
  }
}


void ThreadInfo::remove_inserted_records_of_write_set_from_masstree()
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).op == OP_TYPE::INSERT) {
      MTDB.remove_value((*itr).rec_ptr->tuple.key.get(), (*itr).rec_ptr->tuple.len_key);
      delete (*itr).rec_ptr;
    }
  }
}

ReadSetObj* ThreadInfo::search_read_set(const char* key, std::size_t len_key)
{
  for (auto itr = read_set.begin(); itr != read_set.end(); ++itr) {
    if ((*itr).rec_ptr->tuple.len_key == len_key
        && memcmp((*itr).rec_read.tuple.key.get(), key, len_key) == 0) {
      return &(*itr);
    }
  }
  return nullptr;
}

ReadSetObj* ThreadInfo::search_read_set(Record* rec_ptr)
{
  for (auto itr = read_set.begin(); itr != read_set.end(); ++itr)
    if ((*itr).rec_ptr == rec_ptr) return &(*itr);

  return nullptr;
}

WriteSetObj* ThreadInfo::search_write_set(const char* key, std::size_t len_key)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).rec_ptr->tuple.len_key == len_key
        && memcmp((*itr).rec_ptr->tuple.key.get(), key, len_key) == 0) {
      return &(*itr);
    }
  }
  return nullptr;
}

WriteSetObj* ThreadInfo::search_write_set(const char* key, std::size_t len_key, OP_TYPE op)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).rec_ptr->tuple.len_key == len_key
        && (*itr).op == op
        && memcmp((*itr).rec_ptr->tuple.key.get(), key, len_key) == 0) {
      return &(*itr);
    }
  }
  return nullptr;
}

WriteSetObj* ThreadInfo::search_write_set(Record* rec_ptr)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr)
    if ((*itr).rec_ptr == rec_ptr) return &(*itr);

  return nullptr;
}

void ThreadInfo::unlock_write_set() 
{
  TidWord expected, desired;

  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    expected.obj = loadAcquire(itr->rec_ptr->tidw.obj);
    desired = expected;
    desired.lock = 0;
    storeRelease(itr->rec_ptr->tidw.obj, desired.obj);
  }
}

void ThreadInfo::unlock_write_set(std::vector<WriteSetObj>::iterator begin, std::vector<WriteSetObj>::iterator end) 
{
  TidWord expected, desired;

  for (auto itr = begin; itr != end; ++itr) {
    expected.obj = loadAcquire(itr->rec_ptr->tidw.obj);
    desired = expected;
    desired.lock = 0;
    storeRelease(itr->rec_ptr->tidw.obj, desired.obj);
  }
}

void ThreadInfo::wal(uint64_t ctid)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    LogRecord log(ctid, (*itr).op, (*itr).tuple);
    latest_log_header_.chkSum_ += log.computeChkSum();
    log_set_.emplace_back(std::move(log));
    ++latest_log_header_.logRecNum_;
  }
  
  /**
   * This part includes many write system call.
   * Future work: if this degrades the system performance, it should prepare
   * some buffer (like char*) and do memcpy instead of write system call
   * and do write system call in a batch.
   */
  if (log_set_.size() > KVS_LOG_GC_THRESHOLD) {
    // prepare write header
    latest_log_header_.convertChkSumIntoComplementOnTwo();

    // write header
    logfile_.write((void*)&latest_log_header_, sizeof(LogHeader));

    // write log record
    for (auto itr = log_set_.begin(); itr != log_set_.end(); ++itr) {
      // write tx id, op(operation type), length of key, length of val
      logfile_.write((void*)&(*itr), sizeof((*itr).tid_) + sizeof((*itr).op_) + sizeof((*itr).tuple_.len_key) + sizeof((*itr).tuple_.len_val));

      // write key body
      logfile_.write((void*)(*itr).tuple_.key.get(), (*itr).tuple_.len_key);

      // write val body
      if ((*itr).op_ != OP_TYPE::DELETE)
        logfile_.write((void*)(*itr).tuple_.val.get(), (*itr).tuple_.len_val);
    }
  }

  latest_log_header_.init();
  log_set_.clear();
}

void WriteSetObj::display()
{
  cout << "WriteSetObj::display()" << endl;
  cout << "tuple.len_key : " << tuple.len_key << endl;
  cout << "tuple.lenval : " << tuple.len_val << endl;
  /*
  cout << "tuple.key : " << tuple.key.get() << endl;
  cout << "tuple.val : " << tuple.val.get() << endl;
  */
  cout << "op : " << static_cast<int32_t>(op) << endl;
  cout << "rec_ptr : " << rec_ptr << endl;
}

void WriteSetObj::reset(char const *val, std::size_t len_val)
{
    tuple.len_val = len_val;
    tuple.val.reset();
    tuple.val = std::make_unique<char[]>(len_val);
    memcpy(tuple.val.get(), val, len_val);
}

void WriteSetObj::reset(char const* val, std::size_t len_val, OP_TYPE op, Record* rec_ptr)
{
    tuple.len_val = len_val;
    tuple.val.reset();
    tuple.val = std::make_unique<char[]>(len_val);
    memcpy(tuple.val.get(), val, len_val);
    this->op = op;
    this->rec_ptr = rec_ptr;
}

void print_status(Status status)
{
  switch (status) {
    case Status::WARN_NOT_IN_A_SESSION:
      cout << "WARN_NOT_IN_A_SESSION" << endl;
      break;
    case Status::OK:
      cout << "OK" << endl;
      break;
    case Status::ERR_NOT_FOUND:
      cout << "ERR_NOT_FOUND" << endl;
      break;
    case Status::ERR_INVALID_ARGS:
      cout << "ERR_INVALID_ARGS" << endl;
      break;
    case Status::ERR_VALIDATION:
      cout << "ERR_VALIDATION" << endl;
      break;
    default:
      cout << "UNKNWON_STATUS" << endl;
      break;
  }
}

}
