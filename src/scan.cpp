/**
 * @file scan.cc
 * @detail implement about scan operation.
 */

#include <map>
#include <string_view>

#include "concurrency_control.h"
#include "index.h"
#include "kvs/interface.h"
#include "masstree_beta_wrapper.h"
#include "scheme_local.h"
#include "tuple_local.h"

using namespace shirakami;

namespace shirakami {

Status scan_key(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                const char* const lkey, const std::size_t len_lkey,
                const bool l_exclusive, const char* const rkey,
                const std::size_t len_rkey, const bool r_exclusive,
                std::vector<const Tuple*>& result) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) cc_silo::tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  // as a precaution
  result.clear();
  auto rset_init_size = ti->get_read_set().size();

  std::vector<const Record*> scan_res;
  index_kohler_masstree::get_mtdb().scan(lkey, len_lkey, l_exclusive, rkey, len_rkey, r_exclusive, &scan_res,
            false);

  for (auto&& itr : scan_res) {
    std::string_view key_view = itr->get_tuple().get_key();
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

    const ReadSetObj* inrs = ti->search_read_set(itr);
    if (inrs != nullptr) {
      result.emplace_back(&inrs->get_rec_read().get_tuple());
      continue;
    }
    // if the record was already read/update/insert in the same transaction,
    // the result which is record pointer is notified to caller but
    // don't execute re-read (read_record function).
    // Because in herbrand semantics, the read reads last update even if the
    // update is own.

    ti->get_read_set().emplace_back(const_cast<Record*>(itr));
    Status rr = cc_silo::read_record(ti->get_read_set().back().get_rec_read(),
                            const_cast<Record*>(itr));
    if (rr != Status::OK) {
      return rr;
    }
  }

  if (rset_init_size != ti->get_read_set().size()) {
    for (auto itr = ti->get_read_set().begin() + rset_init_size;
         itr != ti->get_read_set().end(); ++itr) {
      result.emplace_back(&itr->get_rec_read().get_tuple());
    }
  }

  return Status::OK;
}

Status open_scan(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                 const char* const lkey, const std::size_t len_lkey,
                 const bool l_exclusive, const char* const rkey,
                 const std::size_t len_rkey, const bool r_exclusive,
                 ScanHandle& handle) {
  auto* ti = static_cast<ThreadInfo*>(token);
  if (!ti->get_txbegan()) cc_silo::tbegin(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());
  std::vector<const Record*> scan_buf;

  index_kohler_masstree::get_mtdb().scan(lkey, len_lkey, l_exclusive, rkey, len_rkey, r_exclusive, &scan_buf,
            true);

  if (!scan_buf.empty()) {
    /**
     * scan could find any records.
     */
    for (ScanHandle i = 0;; ++i) {
      auto itr = ti->get_scan_cache().find(i);
      if (itr == ti->get_scan_cache().end()) {
        ti->get_scan_cache()[i] = std::move(scan_buf);
        ti->get_scan_cache_itr()[i] = 0;
        /**
         * begin : init about right_end_point_
         */
        std::unique_ptr<char[]> tmp_rkey;  // NOLINT
        if (len_rkey > 0) {
          tmp_rkey = std::make_unique<char[]>(len_rkey);  // NOLINT
          memcpy(tmp_rkey.get(), rkey, len_rkey);
        } else {
          /**
           * todo : discuss when len_rkey == 0
           */
        }
        ti->get_rkey()[i] = std::move(tmp_rkey);
        ti->get_len_rkey()[i] = len_rkey;
        ti->get_r_exclusive()[i] = r_exclusive;
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
  /**
   * scan couldn't find any records.
   */
  return Status::WARN_NOT_FOUND;
}

Status scannable_total_index_size(Token token,  // NOLINT
                                  [[maybe_unused]] Storage storage,
                                  ScanHandle& handle, std::size_t& size) {
  auto* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
    /**
     * the handle was invalid.
     */
    return Status::WARN_INVALID_HANDLE;
  }

  size = ti->get_scan_cache()[handle].size();
  return Status::OK;
}

Status read_from_scan(Token token,  // NOLINT
                      [[maybe_unused]] Storage storage, const ScanHandle handle,
                      Tuple** const tuple) {
  auto* ti = static_cast<ThreadInfo*>(token);
  MasstreeWrapper<Record>::thread_init(sched_getcpu());

  if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
    /**
     * the handle was invalid.
     */
    return Status::WARN_INVALID_HANDLE;
  }

  std::vector<const Record*>& scan_buf = ti->get_scan_cache()[handle];
  std::size_t& scan_index = ti->get_scan_cache_itr()[handle];

  if (scan_buf.size() == scan_index) {
    std::vector<const Record*> new_scan_buf;
    const Tuple* tupleptr(&scan_buf.back()->get_tuple());
    index_kohler_masstree::get_mtdb().scan(tupleptr->get_key().data(), tupleptr->get_key().size(), true,
              ti->get_rkey()[handle].get(), ti->get_len_rkey()[handle],
              ti->get_r_exclusive()[handle], &new_scan_buf, true);

    if (!new_scan_buf.empty()) {
      /**
       * scan could find any records.
       */
      scan_buf.assign(new_scan_buf.begin(), new_scan_buf.end());
      scan_index = 0;
    } else {
      /**
       * scan couldn't find any records.
       */
      return Status::WARN_SCAN_LIMIT;
    }
  }

  auto itr = scan_buf.begin() + scan_index;
  std::string_view key_view = (*itr)->get_tuple().get_key();
  const WriteSetObj* inws =
      ti->search_write_set(key_view.data(), key_view.size());
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

  const ReadSetObj* inrs =
      ti->search_read_set(key_view.data(), key_view.size());
  if (inrs != nullptr) {
    *tuple = const_cast<Tuple*>(&inrs->get_rec_read().get_tuple());
    ++scan_index;
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

  ReadSetObj rsob(*itr);
  Status rr = cc_silo::read_record(rsob.get_rec_read(), *itr);
  if (rr != Status::OK) {
    return rr;
  }
  ti->get_read_set().emplace_back(std::move(rsob));
  *tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
  ++scan_index;

  return Status::OK;
}

Status close_scan(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                  const ScanHandle handle) {
  auto* ti = static_cast<ThreadInfo*>(token);

  auto itr = ti->get_scan_cache().find(handle);
  if (itr == ti->get_scan_cache().end()) {
    return Status::WARN_INVALID_HANDLE;
  }
  ti->get_scan_cache().erase(itr);
  auto index_itr = ti->get_scan_cache_itr().find(handle);
  ti->get_scan_cache_itr().erase(index_itr);
  auto rkey_itr = ti->get_rkey().find(handle);
  ti->get_rkey().erase(rkey_itr);
  auto len_rkey_itr = ti->get_len_rkey().find(handle);
  ti->get_len_rkey().erase(len_rkey_itr);
  auto r_exclusive_itr = ti->get_r_exclusive().find(handle);
  ti->get_r_exclusive().erase(r_exclusive_itr);

  return Status::OK;
}

}  // namespace shirakami
