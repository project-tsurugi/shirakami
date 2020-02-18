/**
 * @file scan.cc
 * @detail implement about scan operation.
 */

#include <map>

#include "include/masstree_wrapper.hh"
#include "include/scheme.hh"
#include "include/xact.hh"

#include "kvs/interface.h"
#include "kvs/scheme.h"

using namespace kvs;

Status
open_scan(Token token, Storage storage,
    const char* const lkey, const std::size_t len_lkey, const bool l_exclusive,
    const char* const rkey, const std::size_t len_rkey, const bool r_exclusive,
    std::size_t& handle)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  std::vector<Record*> scan_buf;

  MTDB.scan(lkey, len_lkey, l_exclusive, rkey, len_rkey, r_exclusive, &scan_buf);

  if (scan_buf.size() > 0) {
    /**
     * scan could find any records.
     */
    for (std::size_t i = 0;; ++i) {
      auto itr = ti->scan_cache_.find(i);
      if (itr == ti->scan_cache_.end()) {
        ti->scan_cache_[i] = scan_buf;
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
read_from_scan(Token token, Storage storage, const std::size_t handle, Tuple** const tuple)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  if (ti->scan_cache_.find(handle) == ti->scan_cache_.end()) {
    /**
     * the handle was invalid.
     */
    return Status::WARN_NOT_FOUND;
  }

  std::vector<Record*>& scan_buf = ti->scan_cache_[handle];

  if (scan_buf.size() == 0) {
    return Status::WARN_SCAN_LIMIT;
  }

  auto itr = scan_buf.begin();
  WriteSetObj* inws = ti->search_write_set((*itr)->tuple.key.get(), (*itr)->tuple.len_key);
  if (inws != nullptr) {
    if (inws->op == OP_TYPE::DELETE) {
      return Status::WARN_ALREADY_DELETE;
    }
    *tuple = &inws->tuple;
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  ReadSetObj* inrs = ti->search_read_set((*itr)->tuple.key.get(), (*itr)->tuple.len_key);
  if (inrs != nullptr) {
    *tuple = &inrs->rec_read.tuple;
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  ReadSetObj rsob(*itr);
  if (Status::OK != read_record(rsob.rec_read, *itr)) {
    abort(token);
    return Status::ERR_ILLEGAL_STATE;
  }
  ti->read_set.emplace_back(std::move(rsob));
  *tuple = &ti->read_set.back().rec_read.tuple;

  return Status::OK;
}

Status
close_scan(Token token, Storage storage, const std::size_t handle)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);

  auto itr = ti->scan_cache_.find(handle);
  if (itr == ti->scan_cache_.end())
    return Status::WARN_NOT_FOUND;
  else
    ti->scan_cache_.erase(itr);

  return Status::OK;
}

