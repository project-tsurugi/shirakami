/**
 * @file thread_info.cpp
 * @brief about scheme
 */

#include "thread_info.h"

#include "atomic_wrapper.h"
#include "garbage_collection.h"
#include "index.h"
#include "log.h"
#include "tuple_local.h"

namespace shirakami {

void ThreadInfo::clean_up_ops_set() {
  read_set.clear();
  write_set.clear();
}

void ThreadInfo::clean_up_scan_caches() {
  scan_cache_.clear();
  scan_cache_itr_.clear();
  rkey_.clear();
  len_rkey_.clear();
  r_exclusive_.clear();
}

[[maybe_unused]] void ThreadInfo::display_read_set() {
  std::cout << "==========" << std::endl;
  std::cout << "start : ThreadInfo::display_read_set()" << std::endl;
  std::size_t ctr(1);
  for (auto&& itr : read_set) {
    std::cout << "Element #" << ctr << " of read set." << std::endl;
    std::cout << "rec_ptr_ : " << itr.get_rec_ptr() << std::endl;
    Record& record = itr.get_rec_read();
    Tuple& tuple = record.get_tuple();
    std::cout << "tidw_ :vv" << record.get_tidw() << std::endl;
    std::string_view key_view;
    std::string_view value_view;
    key_view = tuple.get_key();
    value_view = tuple.get_value();
    std::cout << "key : " << key_view << std::endl;
    std::cout << "key_size : " << key_view.size() << std::endl;
    std::cout << "value : " << value_view << std::endl;
    std::cout << "value_size : " << value_view.size() << std::endl;
    std::cout << "----------" << std::endl;
    ++ctr;
  }
  std::cout << "==========" << std::endl;
}

[[maybe_unused]] void ThreadInfo::display_write_set() {
  std::cout << "==========" << std::endl;
  std::cout << "start : ThreadInfo::display_write_set()" << std::endl;
  std::size_t ctr(1);
  for (auto&& itr : write_set) {
    std::cout << "Element #" << ctr << " of write set." << std::endl;
    std::cout << "rec_ptr_ : " << itr.get_rec_ptr() << std::endl;
    std::cout << "op_ : " << itr.get_op() << std::endl;
    std::string_view key_view;
    std::string_view value_view;
    key_view = itr.get_tuple().get_key();
    value_view = itr.get_tuple().get_value();
    std::cout << "key : " << key_view << std::endl;
    std::cout << "key_size : " << key_view.size() << std::endl;
    std::cout << "value : " << value_view << std::endl;
    std::cout << "value_size : " << value_view.size() << std::endl;
    std::cout << "----------" << std::endl;
    ++ctr;
  }
  std::cout << "==========" << std::endl;
}

Status ThreadInfo::check_delete_after_write(  // NOLINT
    const char* const key_ptr, const std::size_t key_length) {
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    // It can't use lange-based for because it use write_set.erase.
    std::string_view key_view = itr->get_rec_ptr()->get_tuple().get_key();
    if (key_view.size() == key_length &&
        memcmp(key_view.data(), key_ptr, key_length) == 0) {
      write_set.erase(itr);
      return Status::WARN_CANCEL_PREVIOUS_OPERATION;
    }
  }

  return Status::OK;
}

void ThreadInfo::remove_inserted_records_of_write_set_from_masstree() {
  for (auto&& itr : write_set) {
    if (itr.get_op() == OP_TYPE::INSERT) {
      Record* record = itr.get_rec_ptr();
      std::string_view key_view = record->get_tuple().get_key();
      index_kohler_masstree::get_mtdb().remove_value(key_view.data(), key_view.size());

      /**
       * create information for garbage collection.
       */
      std::mutex& mutex_for_gclist =
          garbage_collection::get_mutex_garbage_records_at(gc_handle.get_container_index());
      mutex_for_gclist.lock();
      gc_handle.get_record_container()->emplace_back(itr.get_rec_ptr());
      mutex_for_gclist.unlock();
      tid_word deletetid;
      deletetid.set_lock(false);
      deletetid.set_latest(false);
      deletetid.set_absent(false);
      deletetid.set_epoch(this->get_epoch());
      storeRelease(record->get_tidw().obj_, deletetid.obj_);  // NOLINT
    }
  }
}

ReadSetObj* ThreadInfo::search_read_set(const char* const key_ptr,  // NOLINT
                                        const std::size_t key_length) {
  for (auto&& itr : read_set) {
    const std::string_view key_view = itr.get_rec_ptr()->get_tuple().get_key();
    if (key_view.size() == key_length &&
        memcmp(key_view.data(), key_ptr, key_length) == 0) {
      return &itr;
    }
  }
  return nullptr;
}

ReadSetObj* ThreadInfo::search_read_set(  // NOLINT
    const Record* const rec_ptr) {
  for (auto&& itr : read_set) {
    if (itr.get_rec_ptr() == rec_ptr) return &itr;
  }

  return nullptr;
}

WriteSetObj* ThreadInfo::search_write_set(const char* key_ptr,  // NOLINT
                                          std::size_t key_length) {
  for (auto&& itr : write_set) {
    const Tuple* tuple;  // NOLINT
    if (itr.get_op() == OP_TYPE::UPDATE) {
      tuple = &itr.get_tuple_to_local();
    } else {
      // insert/delete
      tuple = &itr.get_tuple_to_db();
    }
    std::string_view key_view = tuple->get_key();
    if (key_view.size() == key_length &&
        memcmp(key_view.data(), key_ptr, key_length) == 0) {
      return &itr;
    }
  }
  return nullptr;
}

const WriteSetObj* ThreadInfo::search_write_set(  // NOLINT
    const Record* rec_ptr) {
  for (auto& itr : write_set) {
    if (itr.get_rec_ptr() == rec_ptr) return &itr;
  }

  return nullptr;
}

void ThreadInfo::unlock_write_set() {
  tid_word expected{};
  tid_word desired{};

  for (auto& itr : write_set) {
    Record* recptr = itr.get_rec_ptr();
    expected = loadAcquire(recptr->get_tidw().obj_);  // NOLINT
    desired = expected;
    desired.set_lock(false);
    storeRelease(recptr->get_tidw().obj_, desired.obj_);  // NOLINT
  }
}

void ThreadInfo::unlock_write_set(  // NOLINT
    std::vector<WriteSetObj>::iterator begin,
    std::vector<WriteSetObj>::iterator end) {
  tid_word expected;
  tid_word desired;

  for (auto itr = begin; itr != end; ++itr) {
    expected = loadAcquire(itr->get_rec_ptr()->get_tidw().obj_);  // NOLINT
    desired = expected;
    desired.set_lock(false);
    storeRelease(itr->get_rec_ptr()->get_tidw().obj_, desired.obj_);  // NOLINT
  }
}

void ThreadInfo::wal(uint64_t ctid) {
  for (auto&& itr : write_set) {
    if (itr.get_op() == OP_TYPE::UPDATE) {
      log_set_.emplace_back(ctid, itr.get_op(), &itr.get_tuple_to_local());
    } else {
      // insert/delete
      log_set_.emplace_back(ctid, itr.get_op(), &itr.get_tuple_to_db());
    }
    latest_log_header_.add_checksum(
        log_set_.back().compute_checksum());  // NOLINT
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
    logfile_.write(static_cast<void*>(&latest_log_header_),
                   sizeof(Log::LogHeader));

    // write log record
    for (auto&& itr : log_set_) {
      // write tx id, op(operation type)
      logfile_.write(static_cast<void*>(&itr),
                     sizeof(itr.get_tid()) + sizeof(itr.get_op()));

      // common subexpression elimination
      const Tuple* tupleptr = itr.get_tuple();

      std::string_view key_view = tupleptr->get_key();
      // write key_length
      // key_view.size() returns constexpr.
      std::size_t key_size = key_view.size();
      logfile_.write(static_cast<void*>(&key_size), sizeof(key_size));

      // write key_body
      logfile_.write((void*)key_view.data(), key_size);  // NOLINT

      std::string_view value_view = tupleptr->get_value();
      // write value_length
      // value_view.size() returns constexpr.
      std::size_t value_size = value_view.size();
      logfile_.write((void*)value_view.data(), value_size);  // NOLINT

      // write val_body
      if (itr.get_op() != OP_TYPE::DELETE) {
        if (value_size != 0) {
          logfile_.write((void*)value_view.data(), value_size);  // NOLINT
        }
      }
    }
  }

  latest_log_header_.init();
  log_set_.clear();
}

}  // namespace shirakami
