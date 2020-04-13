/**
 * @file scan.cc
 * @detail implement about scan operation.
 */

#include <map>
#include <string_view>

#include "debug.hh"
#include "masstree_wrapper.hh"
#include "scheme.hh"
#include "xact.hh"

#include "kvs/interface.h"
#include "kvs/scheme.h"

using namespace kvs;

namespace kvs {

Status
scan_key(Token token, Storage storage,
    const char* const lkey, const std::size_t len_lkey, const bool l_exclusive,
    const char* const rkey, const std::size_t len_rkey, const bool r_exclusive,
    std::vector<const Tuple*>& result)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  // as a precaution
  result.clear();
  auto rset_init_size = ti->read_set.size();

  std::vector<const Record*> scan_res;
  MTDB.scan(lkey, len_lkey, l_exclusive, rkey, len_rkey, r_exclusive, &scan_res);

  for (auto itr = scan_res.begin(); itr != scan_res.end(); ++itr) {
    std::string_view key_view = (*itr)->get_tuple().get_key();
    WriteSetObj* inws = ti->search_write_set(key_view.data(), key_view.size());
    if (inws != nullptr) {
      if (inws->get_op() == OP_TYPE::DELETE) {
        return Status::WARN_ALREADY_DELETE;
      }
      if (inws->get_op() == OP_TYPE::UPDATE) {
        result.emplace_back(&inws->get_tuple_to_local());
      } else if (inws->get_op() == OP_TYPE::INSERT) {
        result.emplace_back(&inws->get_tuple_to_db());
      } else {
        // error
      }
      continue;
    }

    const ReadSetObj* inrs = ti->search_read_set(*itr);
    if (inrs != nullptr) {
      result.emplace_back(&inrs->get_rec_read().get_tuple());
      continue;
    }
    // if the record was already read/update/insert in the same transaction, 
    // the result which is record pointer is notified to caller but
    // don't execute re-read (read_record function).
    // Because in herbrand semantics, the read reads last update even if the update is own.

    ti->read_set.emplace_back(const_cast<Record*>(*itr));
    Status rr = read_record(ti->read_set.back().get_rec_read(), const_cast<Record*>(*itr));
    if (rr != Status::OK) {
      return rr;
    }
  }

  if (rset_init_size != ti->read_set.size()) {
    for (auto itr = ti->read_set.begin() + rset_init_size; itr != ti->read_set.end(); ++itr) {
      result.emplace_back(&itr->get_rec_read().get_tuple());
    }
  }

  return Status::OK;
}

Status
open_scan(Token token, Storage storage,
    const char* const lkey, const std::size_t len_lkey, const bool l_exclusive,
    const char* const rkey, const std::size_t len_rkey, const bool r_exclusive,
    ScanHandle& handle)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  std::vector<const Record*> scan_buf;

  MTDB.scan(lkey, len_lkey, l_exclusive, rkey, len_rkey, r_exclusive, &scan_buf, true);

  if (scan_buf.size() > 0) {
    /**
     * scan could find any records.
     */
    for (ScanHandle i = 0;; ++i) {
      auto itr = ti->scan_cache_.find(i);
      if (itr == ti->scan_cache_.end()) {
        ti->scan_cache_[i] = std::move(scan_buf);
        ti->scan_cache_itr_[i] = 0;
        /**
         * begin : init about right_end_point_
         */
        std::unique_ptr<char[]> tmp_rkey;
        if (len_rkey > 0) {
          tmp_rkey = make_unique<char[]>(len_rkey);
          memcpy(tmp_rkey.get(), rkey, len_rkey); 
        } else {
          /**
           * todo : discuss when len_rkey == 0
           */
        }
        ti->rkey_[i] = std::move(tmp_rkey);
        ti->len_rkey_[i] = len_rkey;
        ti->r_exclusive_[i] = r_exclusive;
        /**
         * end : init about right_end_point_
         */
        handle = i;
        break;
      }
      if (i == SIZE_MAX) return Status::WARN_SCAN_LIMIT;
    }
    return Status::OK;
  }
  else {
    /**
     * scan couldn't find any records.
     */
    return Status::WARN_NOT_FOUND;
  }
}

Status
scannable_total_index_size(Token token, Storage storage, ScanHandle& handle, std::size_t& size)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  if (ti->scan_cache_.find(handle) == ti->scan_cache_.end()) {
    /**
     * the handle was invalid.
     */
    return Status::WARN_INVALID_HANDLE;
  }

  size = ti->scan_cache_[handle].size();
  return Status::OK;
}

Status
read_from_scan(Token token, Storage storage, const ScanHandle handle, Tuple** const tuple)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  if (ti->scan_cache_.find(handle) == ti->scan_cache_.end()) {
    /**
     * the handle was invalid.
     */
    return Status::WARN_INVALID_HANDLE;
  }

  std::vector<const Record*>& scan_buf = ti->scan_cache_[handle];
  std::size_t& scan_index = ti->scan_cache_itr_[handle];

  if (scan_buf.size() == scan_index) {
    std::vector<const Record*> new_scan_buf;
    const Tuple* tupleptr(&scan_buf.back()->get_tuple());
    MTDB.scan(tupleptr->get_key().data(), tupleptr->get_key().size(), true, ti->rkey_[handle].get(), ti->len_rkey_[handle], ti->r_exclusive_[handle], &new_scan_buf, true);

    if (new_scan_buf.size() > 0) {
      /**
       * scan could find any records.
       */
      scan_buf.assign(new_scan_buf.begin(), new_scan_buf.end());
      scan_index = 0;
    }
    else {
      /**
       * scan couldn't find any records.
       */
      return Status::WARN_SCAN_LIMIT;
    }
  }

  NNN;
  auto itr = scan_buf.begin() + scan_index;
  std::string_view key_view = (*itr)->get_tuple().get_key();
  const WriteSetObj* inws = ti->search_write_set(key_view.data(), key_view.size());
  if (inws != nullptr) {
    if (inws->get_op() == OP_TYPE::DELETE) {
      ++scan_index;
      return Status::WARN_ALREADY_DELETE;
    }
    if (inws->get_op() == OP_TYPE::UPDATE) {
      *tuple = const_cast<Tuple*>(&inws->get_tuple_to_local());
    } else {
      // insert/delete
      *tuple = const_cast<Tuple*>(&inws->get_tuple_to_db());
    }
    ++scan_index;
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  const ReadSetObj* inrs = ti->search_read_set(key_view.data(), key_view.size());
  if (inrs != nullptr) {
    *tuple = const_cast<Tuple*>(&inrs->get_rec_read().get_tuple());
    ++scan_index;
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  ReadSetObj rsob(*itr);
  Status rr = read_record(rsob.get_rec_read(), *itr);
  if (rr != Status::OK) {
    return rr;
  }
  ti->read_set.emplace_back(std::move(rsob));
  *tuple = &ti->read_set.back().get_rec_read().get_tuple();
  ++scan_index;

  return Status::OK;
}

Status
close_scan(Token token, Storage storage, const ScanHandle handle)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);

  auto itr = ti->scan_cache_.find(handle);
  if (itr == ti->scan_cache_.end()) {
    return Status::WARN_INVALID_HANDLE;
  } else {
    ti->scan_cache_.erase(itr);
    auto index_itr = ti->scan_cache_itr_.find(handle);
    ti->scan_cache_itr_.erase(index_itr);
    auto rkey_itr = ti->rkey_.find(handle);
    ti->rkey_.erase(rkey_itr);
    auto len_rkey_itr = ti->len_rkey_.find(handle);
    ti->len_rkey_.erase(len_rkey_itr);
    auto r_exclusive_itr = ti->r_exclusive_.find(handle);
    ti->r_exclusive_.erase(r_exclusive_itr);
  }

  return Status::OK;
}

} // namespace kvs
