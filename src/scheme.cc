/**
 * @file scheme.cc
 * @brief about scheme
 */

#include "atomic_wrapper.hh"
#include "gcollection.hh"
#include "log.hh"
#include "scheme.hh"
#include "xact.hh"

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
 
Status ThreadInfo::check_delete_after_write(const char* const key_ptr, const std::size_t key_length)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    std::string_view key_view = itr->get_rec_ptr()->get_tuple().get_key();
    if (key_view.size() == key_length
        && memcmp(key_view.data(), key_ptr, key_length) == 0) {
      write_set.erase(itr);
      return Status::WARN_CANCEL_PREVIOUS_OPERATION;
    }
  }

  return Status::OK;
}

void ThreadInfo::remove_inserted_records_of_write_set_from_masstree()
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if (itr->get_op() == OP_TYPE::INSERT) {
      Record* record = itr->get_rec_ptr();
      std::string_view key_view = record->get_tuple().get_key();
      MTDB.remove_value(key_view.data(), key_view.size());
      
      /**
       * create information for garbage collection.
       */
      std::mutex& mutex_for_gclist = kMutexGarbageRecords[gc_container_index_];
      mutex_for_gclist.lock();
      gc_record_container_->emplace_back(itr->get_rec_ptr());
      mutex_for_gclist.unlock();
      TidWord deletetid;
      deletetid.set_lock(false);
      deletetid.set_latest(false);
      deletetid.set_absent(false);
      deletetid.set_epoch(this->get_epoch());
      storeRelease(record->get_tidw(), deletetid);
    }
  }
}

ReadSetObj* ThreadInfo::search_read_set(const char* const key_ptr, const std::size_t key_length)
{
  for (auto itr = read_set.begin(); itr != read_set.end(); ++itr) {
    const std::string_view key_view = itr->get_rec_ptr()->get_tuple_ptr()->get_key();
    if (key_view.size() == key_length
        && memcmp(key_view.data(), key_ptr, key_length) == 0) {
      return &(*itr);
    }
  }
  return nullptr;
}

ReadSetObj* ThreadInfo::search_read_set(const Record* const rec_ptr)
{
  for (auto itr = read_set.begin(); itr != read_set.end(); ++itr)
    if (itr->get_rec_ptr() == rec_ptr) return &(*itr);

  return nullptr;
}

WriteSetObj* ThreadInfo::search_write_set(const char* key_ptr, const std::size_t key_length)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    const Tuple* tuple;
    if (itr->get_op() == OP_TYPE::UPDATE) {
      tuple = itr->get_tuple_ptr_to_local();
    } else {
      // insert/delete
      tuple = itr->get_tuple_ptr_to_db();
    }
    // Rvalue reference suppresses useless copying.
    std::string_view&& key_view = tuple->get_key();
    if (key_view.size() == key_length
        && memcmp(key_view.data(), key_ptr, key_length) == 0) {
      return &(*itr);
    }
  }
  return nullptr;
}

const WriteSetObj* ThreadInfo::search_write_set(const Record* const rec_ptr)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr)
    if (itr->get_rec_ptr() == rec_ptr) return &(*itr);

  return nullptr;
}

void ThreadInfo::unlock_write_set() 
{
  TidWord expected, desired;

  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    Record* recptr = itr->get_rec_ptr();
    expected = loadAcquire(recptr->get_tidw_ref());
    desired = expected;
    desired.set_lock(false);
    storeRelease(recptr->get_tidw_ref(), desired);
  }
}

void ThreadInfo::unlock_write_set(std::vector<WriteSetObj>::iterator begin, std::vector<WriteSetObj>::iterator end) 
{
  TidWord expected, desired;

  for (auto itr = begin; itr != end; ++itr) {
    expected = loadAcquire(itr->get_rec_ptr()->get_tidw_ref());
    desired = expected;
    desired.set_lock(0);
    storeRelease(itr->get_rec_ptr()->get_tidw_ref(), desired);
  }
}

void ThreadInfo::wal(uint64_t ctid)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if (itr->get_op() == OP_TYPE::UPDATE) {
      log_set_.emplace_back(ctid, itr->get_op(), itr->get_tuple_ptr_to_local());
    } else {
      // insert/delete
      log_set_.emplace_back(ctid, itr->get_op(), itr->get_tuple_ptr_to_db());
    }
    latest_log_header_.chkSum_ += log.log_set_.back().computeChkSum();
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
        if ((*itr).tuple_.len_val != 0) {
          logfile_.write((void*)(*itr).tuple_.val.get(), (*itr).tuple_.len_val);
        }
    }
  }

  latest_log_header_.init();
  log_set_.clear();
}

bool 
operator<(const WriteSetObj& right) const 
{
  const Tuple* this_tuple_ptr;
  if (this->op_ == OP_TYPE::UPDATE) {
    this_tuple_ptr = this->get_tuple_ptr_to_local();
  } else {
    // insert/delete
    this_tuple_ptr = this->get_tuple_ptr_to_db();
  }
  const Tuple* right_tuple_ptr;
  if (this->op_ == OP_TYPE::UPDATE) {
    right_tuple_ptr = right.get_tuple_ptr_to_local();
  } else {
    // insert/delete
    right_tuple_ptr = right.get_tuple_ptr_to_db();
  }

  const char* this_key_ptr(this_tuple_ptr->get_key().data());
  const char* right_key_ptr(right_tuple_ptr->get_key().data());
  std::size_t this_key_size(this_tuple_ptr->get_key().size());
  std::size_t right_key_size(right_tuple_ptr->get_key().size());

  bool judge = false;
  if (this_key_size < right_key_size) {
    if (memcmp(this_key_ptr, right_key_ptr, this_key_size) <= 0) {
      return true;
    } else {
      return false;
    }
  } else if (this_key_size > right_key_size) {
    if (memcmp(this_key_ptr, right_key_ptr, right_key_size) < 0) {
      return true;
    } else {
      return false;
    }
  } else { // same length
    int ret = memcmp(this_key_ptr, right_key_ptr, this_key_size);      
    if (ret < 0) {
      return true;
    } else if (ret > 0) {
      return false;
    } else {
      ERR; // Unique key is not allowed now.
    }
  }
}

void
WriteSetObj::reset_tuple(const char* const val_ptr, const std::size_t val_length)
{
  if (itr->op_ == OP_TYPE::UPDATE) {
    itr->get_tuple_ptr_to_local()->set(val_ptr, val_length);
  } else {
    // insert
    tuple = itr->get_tuple_ptr_to_db()->set(val_ptr, val_length);
  }
}

} // namespace kvs
