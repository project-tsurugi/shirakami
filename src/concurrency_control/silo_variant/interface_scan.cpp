/**
 * @file interface_scan.cpp
 * @detail implement about scan operation.
 */

#include <map>

#include "concurrency_control/silo_variant/include/interface_helper.h"

#ifdef INDEX_KOHLER_MASSTREE

#include "index/masstree_beta/include/masstree_beta_wrapper.h"

#elif defined(INDEX_YAKUSHIMA)

#include "index/yakushima/include/scheme.h"

#endif

#include "tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant {

Status close_scan(Token token, ScanHandle handle) {  // NOLINT
    auto* ti = static_cast<session_info*>(token);

    auto itr = ti->get_scan_cache().find(handle);
    if (itr == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }
    ti->get_scan_cache().erase(itr);
    auto index_itr = ti->get_scan_cache_itr().find(handle);
    ti->get_scan_cache_itr().erase(index_itr);
    auto r_key_itr = ti->get_r_key().find(handle);
    ti->get_r_key().erase(r_key_itr);
    auto r_exclusive_itr = ti->get_r_end().find(handle);
    ti->get_r_end().erase(r_exclusive_itr);

    return Status::OK;
}

Status open_scan(Token token, const std::string_view l_key,  // NOLINT
                 const scan_endpoint l_end, const std::string_view r_key,
                 const scan_endpoint r_end, ScanHandle &handle) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token);

#ifdef INDEX_KOHLER_MASSTREE
    std::vector<const Record*> scan_buf;
    masstree_wrapper<Record>::thread_init(sched_getcpu());
    kohler_masstree::get_mtdb().scan(l_key, l_end, r_key, r_end, &scan_buf, true);
#elif defined(INDEX_YAKUSHIMA)
    std::vector<std::pair<Record**, std::size_t>> scan_res;
    std::vector<
            std::pair<yakushima::node_version64_body, yakushima::node_version64*>>
            nvec;
    yakushima::scan(l_key, parse_scan_endpoint(l_end), r_key, parse_scan_endpoint(r_end), scan_res, &nvec);
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
                if (!r_key.empty()) {
                    ti->get_r_key()[i] = r_key;
                } else {
                    ti->get_r_key()[i] = "";
                }
                ti->get_r_end()[i] = r_end;
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

Status read_from_scan(Token token, ScanHandle handle,  // NOLINT
                      Tuple** const tuple) {
    auto* ti = static_cast<session_info*>(token);

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }

#if defined(INDEX_YAKUSHIMA)
    std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>> &scan_buf = ti->get_scan_cache()[handle];
    std::size_t &scan_index = ti->get_scan_cache_itr()[handle];
    if (scan_buf.size() == scan_index) {
        const Tuple* tupleptr(&std::get<0>(scan_buf.back())->get_tuple());
#elif defined(INDEX_KOHLER_MASSTREE)
        std::vector<const Record*> &scan_buf = ti->get_scan_cache()[handle];
        std::size_t &scan_index = ti->get_scan_cache_itr()[handle];
        if (scan_buf.size() == scan_index) {
            const Tuple* tupleptr(&(scan_buf.back())->get_tuple());
#endif

#if defined(INDEX_KOHLER_MASSTREE)
        std::vector<const Record*> new_scan_buf;
        masstree_wrapper<Record>::thread_init(sched_getcpu());
        kohler_masstree::get_mtdb().scan(tupleptr->get_key(), scan_endpoint::EXCLUSIVE, ti->get_r_key()[handle],
                                         ti->get_r_end()[handle], &new_scan_buf, true);
#elif defined(INDEX_YAKUSHIMA)
        std::vector<std::pair<Record**, std::size_t>> scan_res;
        std::vector<std::pair<yakushima::node_version64_body, yakushima::node_version64*>> nvec;
        yakushima::scan(tupleptr->get_key(), parse_scan_endpoint(scan_endpoint::EXCLUSIVE), ti->get_r_key()[handle],
                        parse_scan_endpoint(ti->get_r_end()[handle]), scan_res, &nvec);
        std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>> new_scan_buf;
        new_scan_buf.reserve(scan_res.size());
        for (std::size_t i = 0; i < scan_res.size(); ++i) {
            new_scan_buf.emplace_back(*scan_res.at(i).first, nvec.at(i).first, nvec.at(i).second);
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
#elif defined(INDEX_KOHLER_MASSTREE)
    std::string_view key_view = (*itr)->get_tuple().get_key();
#endif
    /**
     * Check read-own-write
     */
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

#ifdef INDEX_YAKUSHIMA
    read_set_obj rsob(std::get<0>(*itr), true, std::get<1>(*itr),
                      std::get<2>(*itr));
    Status rr = read_record(rsob.get_rec_read(), std::get<0>(*itr));
#elif defined(INDEX_KOHLER_MASSTREE)
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

Status scan_key(Token token, const std::string_view l_key, const scan_endpoint l_end,  // NOLINT
                const std::string_view r_key, const scan_endpoint r_end, std::vector<const Tuple*> &result) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token);
    // as a precaution
    result.clear();
    auto rset_init_size = ti->get_read_set().size();

#ifdef INDEX_KOHLER_MASSTREE
    std::vector<const Record*> scan_res;
    masstree_wrapper<Record>::thread_init(sched_getcpu());
    kohler_masstree::get_mtdb().scan(l_key, l_end, r_key, r_end, &scan_res, false);
#elif defined(INDEX_YAKUSHIMA)
    std::vector<std::pair<Record**, std::size_t>> scan_buf;
    std::vector<std::pair<yakushima::node_version64_body, yakushima::node_version64*>> nvec;
    yakushima::scan(l_key, parse_scan_endpoint(l_end), r_key, parse_scan_endpoint(r_end), scan_buf, &nvec);
    std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>> scan_res;
    scan_res.reserve(scan_buf.size());
    for (std::size_t i = 0; i < scan_buf.size(); ++i) {
        scan_res.emplace_back(*scan_buf.at(i).first, nvec.at(i).first, nvec.at(i).second);
    }
#endif

    for (auto &&itr : scan_res) {
#ifdef INDEX_YAKUSHIMA
        write_set_obj* inws =
                ti->search_write_set(std::get<0>(itr)->get_tuple().get_key());
#elif defined(INDEX_KOHLER_MASSTREE)
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

        // if the record was already update/insert in the same transaction,
        // the result which is record pointer is notified to caller but
        // don't execute re-read (read_record function).
        // Because in herbrand semantics, the read reads last update even if the
        // update is own.

#ifdef INDEX_YAKUSHIMA
        ti->get_read_set().emplace_back(const_cast<Record*>(std::get<0>(itr)), true,
                                        std::get<1>(itr), std::get<2>(itr));
        Status rr = read_record(ti->get_read_set().back().get_rec_read(),
                                const_cast<Record*>(std::get<0>(itr)));
#elif defined(INDEX_KOHLER_MASSTREE)
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

[[maybe_unused]] Status scannable_total_index_size(Token token,  // NOLINT
                                                   ScanHandle handle,
                                                   std::size_t &size) {
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
