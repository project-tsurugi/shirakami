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

void 
ThreadInfo::clean_up_ops_set()
{
  read_set.clear();
  write_set.clear();
  opr_set.clear();
}

void
ThreadInfo::clean_up_scan_caches()
{
  scan_cache_.clear();
  scan_cache_itr_.clear();
  rkey_.clear();
  len_rkey_.clear();
  r_exclusive_.clear();
}
 
void
ThreadInfo::display_read_set()
{
  cout << "==========" << endl;
  cout << "start : ThreadInfo::display_read_set()" << endl;
  std::size_t ctr(1);
  for (auto itr = read_set.begin(); itr != read_set.end(); ++itr) {
    cout << "Element #" << ctr << " of read set." << endl;
    cout << "rec_ptr_ : " << itr->get_rec_ptr() << endl;
    Record& record = itr->get_rec_read();
    Tuple& tuple = record.get_tuple();
    cout << "tidw_ :vv" << record.get_tidw() << endl;
    std::string_view key_view, value_view;
    key_view = tuple.get_key();
    value_view = tuple.get_value();
    cout << "key : " << key_view << endl;
    cout << "key_size : " << key_view.size() << endl;
    cout << "value : " << value_view << endl;
    cout << "value_size : " << value_view.size() << endl;
    cout << "----------" << endl;
    ++ctr;
  }
  cout << "==========" << endl;
}

void
ThreadInfo::display_write_set()
{
  cout << "==========" << endl;
  cout << "start : ThreadInfo::display_write_set()" << endl;
  std::size_t ctr(1);
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    cout << "Element #" << ctr << " of write set." << endl;
    cout << "rec_ptr_ : " << itr->get_rec_ptr() << endl;
    cout << "op_ : " << itr->get_op() << endl;
    std::string_view key_view, value_view;
    key_view = itr->get_tuple().get_key();
    value_view = itr->get_tuple().get_value();
    cout << "key : " << key_view << endl;
    cout << "key_size : " << key_view.size() << endl;
    cout << "value : " << value_view << endl;
    cout << "value_size : " << value_view.size() << endl;
    cout << "----------" << endl;
    ++ctr;
  }
  cout << "==========" << endl;
}

Status
ThreadInfo::check_delete_after_write(const char* const key_ptr, const std::size_t key_length)
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

void
ThreadInfo::remove_inserted_records_of_write_set_from_masstree()
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
      storeRelease(record->get_tidw().obj_, deletetid.obj_);
    }
  }
}

ReadSetObj* ThreadInfo::search_read_set(const char* const key_ptr, const std::size_t key_length)
{
  for (auto itr = read_set.begin(); itr != read_set.end(); ++itr) {
    const std::string_view key_view = itr->get_rec_ptr()->get_tuple().get_key();
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
      tuple = &itr->get_tuple_to_local();
    } else {
      // insert/delete
      tuple = &itr->get_tuple_to_db();
    }
    std::string_view key_view = tuple->get_key();
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
    expected = loadAcquire(recptr->get_tidw().obj_);
    desired = expected;
    desired.set_lock(false);
    storeRelease(recptr->get_tidw().obj_, desired.obj_);
  }
}

void ThreadInfo::unlock_write_set(std::vector<WriteSetObj>::iterator begin, std::vector<WriteSetObj>::iterator end) 
{
  TidWord expected, desired;

  for (auto itr = begin; itr != end; ++itr) {
    expected = loadAcquire(itr->get_rec_ptr()->get_tidw().obj_);
    desired = expected;
    desired.set_lock(0);
    storeRelease(itr->get_rec_ptr()->get_tidw().obj_, desired.obj_);
  }
}

void ThreadInfo::wal(uint64_t ctid)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if (itr->get_op() == OP_TYPE::UPDATE) {
      log_set_.emplace_back(ctid, itr->get_op(), &itr->get_tuple_to_local());
    } else {
      // insert/delete
      log_set_.emplace_back(ctid, itr->get_op(), &itr->get_tuple_to_db());
    }
    latest_log_header_.add_checksum(log_set_.back().compute_checksum());
    latest_log_header_.inc_log_rec_num();
  }
  
  /**
   * This part includes many write system call.
   * Future work: if this degrades the system performance, it should prepare
   * some buffer (like char*) and do memcpy instead of write system call
   * and do write system call in a batch.
   */
  if (log_set_.size() > KVS_LOG_GC_THRESHOLD) {
    // prepare write header
    latest_log_header_.compute_two_complement_of_checksum();

    // write header
    logfile_.write((void*)&latest_log_header_, sizeof(LogHeader));

    // write log record
    for (auto itr = log_set_.begin(); itr != log_set_.end(); ++itr) {
      // write tx id, op(operation type)
      logfile_.write((void*)&(*itr), sizeof(itr->get_tid()) + sizeof(itr->get_op()));

      // common subexpression elimination
      const Tuple* tupleptr = itr->get_tuple();

      std::string_view key_view = tupleptr->get_key();
      // write key_length
      // key_view.size() returns constexpr.
      std::size_t key_size = key_view.size();
      logfile_.write((void*)&key_size, sizeof(key_size));

      // write key_body
      logfile_.write((void*)key_view.data(), key_size);

      std::string_view value_view = tupleptr->get_value();
      // write value_length
      // value_view.size() returns constexpr.
      std::size_t value_size = value_view.size();
      logfile_.write((void*)value_view.data(), value_size);

      // write val_body
      if (itr->get_op() != OP_TYPE::DELETE)
        if (value_size != 0) {
          logfile_.write((void*)value_view.data(), value_size);
        }
    }
  }

  latest_log_header_.init();
  log_set_.clear();
}

bool 
WriteSetObj::operator<(const WriteSetObj& right) const 
{
  const Tuple& this_tuple = this->get_tuple(this->get_op());
  const Tuple& right_tuple = right.get_tuple(right.get_op());

  const char* this_key_ptr(this_tuple.get_key().data());
  const char* right_key_ptr(right_tuple.get_key().data());
  std::size_t this_key_size(this_tuple.get_key().size());
  std::size_t right_key_size(right_tuple.get_key().size());

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
WriteSetObj::reset_tuple_value(const char* const val_ptr, const std::size_t val_length) & 
{
  if (this->get_op() == OP_TYPE::UPDATE) {
    this->get_tuple_to_local().set_value(val_ptr, val_length);
  } else {
    // insert
    this->get_tuple_to_db().set_value(val_ptr, val_length);
  }
}

} // namespace kvs
