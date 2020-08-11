/**
 * @file interface_scan.cpp
 * @detail implement about scan operation.
 */

#include <map>

#include "cc/silo_variant/include/interface_helper.h"
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant {

Status close_scan(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                  ScanHandle handle) {
  auto* ti = static_cast<session_info*>(token);

  auto itr = ti->get_scan_cache().find(handle);
  if (itr == ti->get_scan_cache().end()) {
    return Status::WARN_INVALID_HANDLE;
  }
  ti->get_scan_cache().erase(itr);
  auto index_itr = ti->get_scan_cache_itr().find(handle);
  ti->get_scan_cache_itr().erase(index_itr);
  auto r_key_itr = ti->get_rkey().find(handle);
  ti->get_rkey().erase(r_key_itr);
  auto len_r_key_itr = ti->get_len_rkey().find(handle);
  ti->get_len_rkey().erase(len_r_key_itr);
  auto r_exclusive_itr = ti->get_r_exclusive().find(handle);
  ti->get_r_exclusive().erase(r_exclusive_itr);

  return Status::OK;
}

Status open_scan(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                 std::string_view left_key, bool l_exclusive,
                 std::string_view right_key, bool r_exclusive,
                 ScanHandle& handle) {
  auto* ti = static_cast<session_info*>(token);
  if (!ti->get_txbegan()) tx_begin(token);

#ifdef INDEX_KOHLER_MASSTREE
  std::vector<const Record*> scan_buf;
  masstree_wrapper<Record>::thread_init(sched_getcpu());
  kohler_masstree::get_mtdb().scan(
      left_key.size() == 0 ? nullptr : left_key.data(), left_key.size(),
      l_exclusive, right_key.size() == 0 ? nullptr : right_key.data(),
      right_key.size(), r_exclusive, &scan_buf, true);
#elif INDEX_YAKUSHIMA
  std::vector<std::pair<Record**, std::size_t>> scan_res;
  std::vector<
      std::pair<yakushima::node_version64_body, yakushima::node_version64*>>
      nvec;
  yakushima::yakushima_kvs::scan(left_key, l_exclusive,  // NOLINT
                                 right_key, r_exclusive, scan_res, &nvec);
  std::vector<std::tuple<const Record*, yakushima::node_version64_body,
                         yakushima::node_version64*>>
      scan_buf;
  scan_buf.reserve(scan_res.size());
  for (std::size_t i = 0; i < scan_res.size(); ++i) {
    scan_buf.emplace_back(*scan_res.at(i).first, nvec.at(i).first,
                          nvec.at(i).second);
  }
#endif

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
        if (!right_key.empty()) {
          tmp_rkey = std::make_unique<char[]>(right_key.size());  // NOLINT
          memcpy(tmp_rkey.get(), right_key.data(), right_key.size());
        } else {
          /**
           * todo : discuss when len_rkey == 0
           */
        }
        ti->get_rkey()[i] = std::move(tmp_rkey);
        ti->get_len_rkey()[i] = right_key.size();
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

Status read_from_scan(Token token,  // NOLINT
                      [[maybe_unused]] Storage storage, ScanHandle handle,
                      Tuple** tuple) {
  auto* ti = static_cast<session_info*>(token);

  if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
    /**
     * the handle was invalid.
     */
    return Status::WARN_INVALID_HANDLE;
  }

#ifdef INDEX_YAKUSHIMA
  std::vector<std::tuple<const Record*, yakushima::node_version64_body,
                         yakushima::node_version64*>>& scan_buf =
      ti->get_scan_cache()[handle];
  std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
  if (scan_buf.size() == scan_index) {
    const Tuple* tupleptr(&std::get<0>(scan_buf.back())->get_tuple());
#elif INDEX_KOHLER_MASSTREE
  std::vector<const Record*>& scan_buf = ti->get_scan_cache()[handle];
  std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
  if (scan_buf.size() == scan_index) {
    const Tuple* tupleptr(&(scan_buf.back())->get_tuple());
#endif

#ifdef INDEX_KOHLER_MASSTREE
    std::vector<const Record*> new_scan_buf;
    masstree_wrapper<Record>::thread_init(sched_getcpu());
    kohler_masstree::get_mtdb().scan(
        tupleptr->get_key().size() == 0 ? nullptr : tupleptr->get_key().data(),
        tupleptr->get_key().size(), true,
        ti->get_len_rkey()[handle] == 0 ? nullptr
                                        : ti->get_rkey()[handle].get(),
        ti->get_len_rkey()[handle], ti->get_r_exclusive()[handle],
        &new_scan_buf, true);
#elif INDEX_YAKUSHIMA
    std::vector<std::pair<Record**, std::size_t>> scan_res;
    std::vector<
        std::pair<yakushima::node_version64_body, yakushima::node_version64*>>
        nvec;
    yakushima::yakushima_kvs::scan(  // NOLINT
        {tupleptr->get_key().data(), tupleptr->get_key().size()}, true,
        {ti->get_rkey()[handle].get(), ti->get_len_rkey()[handle]},
        ti->get_r_exclusive()[handle], scan_res, &nvec);
    std::vector<std::tuple<const Record*, yakushima::node_version64_body,
                           yakushima::node_version64*>>
        new_scan_buf;
    new_scan_buf.reserve(scan_res.size());
    for (std::size_t i = 0; i < scan_res.size(); ++i) {
      new_scan_buf.emplace_back(*scan_res.at(i).first, nvec.at(i).first,
                                nvec.at(i).second);
    }
#endif

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
#ifdef INDEX_YAKUSHIMA
  std::string_view key_view = std::get<0>(*itr)->get_tuple().get_key();
#elif INDEX_KOHLER_MASSTREE
  std::string_view key_view = (*itr)->get_tuple().get_key();
#endif
  const write_set_obj* inws = ti->search_write_set(key_view);
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

  const read_set_obj* inrs = ti->search_read_set(key_view);
  if (inrs != nullptr) {
    *tuple = const_cast<Tuple*>(&inrs->get_rec_read().get_tuple());
    ++scan_index;
    return Status::WARN_READ_FROM_OWN_OPERATION;
  }

#ifdef INDEX_YAKUSHIMA
  read_set_obj rsob(std::get<0>(*itr), true, std::get<1>(*itr),
                  std::get<2>(*itr));
  Status rr = read_record(rsob.get_rec_read(), std::get<0>(*itr));
#elif INDEX_KOHLER_MASSTREE
  read_set_obj rsob(*itr, true);
  Status rr = read_record(rsob.get_rec_read(), *itr);
#endif
  if (rr != Status::OK) {
    return rr;
  }
  ti->get_read_set().emplace_back(std::move(rsob));
  *tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
  ++scan_index;

  return Status::OK;
}

Status scan_key(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                std::string_view left_key, bool l_exclusive,
                std::string_view right_key, bool r_exclusive,
                std::vector<const Tuple*>& result) {
  auto* ti = static_cast<session_info*>(token);
  if (!ti->get_txbegan()) tx_begin(token);
  // as a precaution
  result.clear();
  auto rset_init_size = ti->get_read_set().size();

#ifdef INDEX_KOHLER_MASSTREE
  std::vector<const Record*> scan_res;
  masstree_wrapper<Record>::thread_init(sched_getcpu());
  kohler_masstree::get_mtdb().scan(
      left_key.size() == 0 ? nullptr : left_key.data(), left_key.size(),
      l_exclusive, right_key.size() == 0 ? nullptr : right_key.data(),
      right_key.size(), r_exclusive, &scan_res, false);
#elif INDEX_YAKUSHIMA
  std::vector<std::pair<Record**, std::size_t>> scan_buf;
  std::vector<
      std::pair<yakushima::node_version64_body, yakushima::node_version64*>>
      nvec;
  yakushima::yakushima_kvs::scan(left_key, l_exclusive,  // NOLINT
                                 right_key, r_exclusive, scan_buf, &nvec);
  std::vector<std::tuple<const Record*, yakushima::node_version64_body,
                         yakushima::node_version64*>>
      scan_res;
  scan_res.reserve(scan_buf.size());
  for (std::size_t i = 0; i < scan_buf.size(); ++i) {
    scan_res.emplace_back(*scan_buf.at(i).first, nvec.at(i).first,
                          nvec.at(i).second);
  }
#endif

  for (auto&& itr : scan_res) {
#ifdef INDEX_YAKUSHIMA
    write_set_obj* inws =
        ti->search_write_set(std::get<0>(itr)->get_tuple().get_key());
#elif INDEX_KOHLER_MASSTREE
    write_set_obj* inws = ti->search_write_set(itr->get_tuple().get_key());
#endif
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

#ifdef INDEX_YAKUSHIMA
    const read_set_obj* inrs = ti->search_read_set(std::get<0>(itr));
#elif INDEX_KOHLER_MASSTREE
    const read_set_obj* inrs = ti->search_read_set(itr);
#endif
    if (inrs != nullptr) {
      result.emplace_back(&inrs->get_rec_read().get_tuple());
      continue;
    }
    // if the record was already read/update/insert in the same transaction,
    // the result which is record pointer is notified to caller but
    // don't execute re-read (read_record function).
    // Because in herbrand semantics, the read reads last update even if the
    // update is own.

#ifdef INDEX_YAKUSHIMA
    ti->get_read_set().emplace_back(const_cast<Record*>(std::get<0>(itr)), true,
                                    std::get<1>(itr), std::get<2>(itr));
    Status rr = read_record(ti->get_read_set().back().get_rec_read(),
                            const_cast<Record*>(std::get<0>(itr)));
#elif INDEX_KOHLER_MASSTREE
    ti->get_read_set().emplace_back(const_cast<Record*>(itr), true);
    Status rr = read_record(ti->get_read_set().back().get_rec_read(),
                            const_cast<Record*>(itr));
#endif
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

[[maybe_unused]] Status scannable_total_index_size(  // NOLINT
    Token token,                                     // NOLINT
    [[maybe_unused]] Storage storage, ScanHandle& handle, std::size_t& size) {
  auto* ti = static_cast<session_info*>(token);
#ifdef INDEX_KOHLER_MASSTREE
  masstree_wrapper<Record>::thread_init(sched_getcpu());
#endif

  if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
    /**
     * the handle was invalid.
     */
    return Status::WARN_INVALID_HANDLE;
  }

  size = ti->get_scan_cache()[handle].size();
  return Status::OK;
}

}  // namespace shirakami::cc_silo_variant
