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

  if (ti->scan_cache_.size() > 0) {
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
    return Status::WARN_NOT_FOUND;
  }
}

Status
read_from_scan(Token token, Storage storage, const std::size_t handle, const std::size_t n_read, std::vector<Tuple*>& result)
{
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  result.clear();

  if (ti->scan_cache_.find(handle) == ti->scan_cache_.end())
    return Status::WARN_NOT_FOUND;
  std::vector<Record*>& scan_buf = ti->scan_cache_[handle];

  std::size_t ctr_read(0);
  for (auto itr = scan_buf.begin(); itr != scan_buf.end();  ++itr) {
    Tuple* tuple;
    search_key(token, storage, (*itr)->tuple.key.get(), (*itr)->tuple.len_key, &tuple);
    result.emplace_back(tuple);
    ++ctr_read;
    if (ctr_read == n_read) {
      scan_buf.erase(scan_buf.begin(), scan_buf.begin()+ctr_read);
      return Status::OK;
    }
  }

  // read all records in scan_buf.
  scan_buf.clear();
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

