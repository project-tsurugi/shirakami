/**
 * @file interface_scan.cpp
 * @detail implement about scan operation.
 */

#include <map>

#include "concurrency_control/silo_variant/include/interface_helper.h"
#include "concurrency_control/silo_variant/include/snapshot_interface.h"

#include "index/yakushima/include/scheme.h"

#include "kvs/interface.h"

#include "logger.h"
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

    return Status::OK;
}

Status open_scan(Token token, const std::string_view l_key,  // NOLINT
                 const scan_endpoint l_end, const std::string_view r_key,
                 const scan_endpoint r_end, ScanHandle &handle) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::open_scan(ti, l_key, l_end, r_key, r_end, handle);
    }

    for (ScanHandle i = 0;; ++i) {
        auto itr = ti->get_scan_cache().find(i);
        if (itr == ti->get_scan_cache().end()) {
            handle = i;
            ti->get_scan_cache_itr()[i] = 0;
            break;
        }
        if (i == SIZE_MAX) return Status::WARN_SCAN_LIMIT;
    }

    std::vector<std::pair<Record**, std::size_t>> scan_res;
    std::vector<std::pair<yakushima::node_version64_body, yakushima::node_version64*>> nvec;
    yakushima::scan(l_key, parse_scan_endpoint(l_end), r_key, parse_scan_endpoint(r_end), scan_res, &nvec);
    if (scan_res.empty()) {
        /**
         * scan couldn't find any records.
         */
        return Status::WARN_NOT_FOUND;
    }

    ti->get_scan_cache()[handle].reserve(scan_res.size());
    for (std::size_t i = 0; i < scan_res.size(); ++i) {
        ti->get_scan_cache()[handle].emplace_back(*scan_res.at(i).first, nvec.at(i).first, nvec.at(i).second);
    }

    return Status::OK;
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

    std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>> &scan_buf = ti->get_scan_cache()[handle];
    std::size_t &scan_index = ti->get_scan_cache_itr()[handle];
    if (scan_buf.size() == scan_index) {
        return Status::WARN_SCAN_LIMIT;
    }

    auto itr = scan_buf.begin() + scan_index;
    std::string_view key_view = std::get<0>(*itr)->get_tuple().get_key();
    /**
     * Check read-own-write
     */
    const write_set_obj* inws = ti->search_write_set(key_view);
    if (inws != nullptr) {
        ++scan_index;
        if (inws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        if (inws->get_op() == OP_TYPE::UPDATE) {
            *tuple = const_cast<Tuple*>(&inws->get_tuple_to_local());
        } else {
            // insert/delete
            *tuple = const_cast<Tuple*>(&inws->get_tuple_to_db());
        }
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    read_set_obj rsob(std::get<0>(*itr));
    if (ti->get_node_set().empty() ||
        std::get<1>(ti->get_node_set().back()) != std::get<2>(*itr)) {
        ti->get_node_set().emplace_back(std::get<1>(*itr), std::get<2>(*itr));
    }

    // pre-verify of phantom problem.
    if (std::get<0>(ti->get_node_set().back()) != std::get<1>(ti->get_node_set().back())->get_stable_version()) {
        cc_silo_variant::abort(token);
        return Status::ERR_PHANTOM;
    }

    Status rr = read_record(rsob.get_rec_read(), std::get<0>(*itr));
    if (rr != Status::OK) return rr;
    ti->get_read_set().emplace_back(std::move(rsob));
    *tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
    ++scan_index;

    return Status::OK;
}

Status scan_key(Token token, const std::string_view l_key, const scan_endpoint l_end,  // NOLINT
                const std::string_view r_key, const scan_endpoint r_end, std::vector<const Tuple*> &result) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::scan_key(ti, l_key, l_end, r_key, r_end, result);
    }

    // as a precaution
    result.clear();
    auto rset_init_size = ti->get_read_set().size();

    std::vector<std::pair<Record**, std::size_t>> scan_buf;
    std::vector<std::pair<yakushima::node_version64_body, yakushima::node_version64*>> nvec;
    yakushima::scan(l_key, parse_scan_endpoint(l_end), r_key, parse_scan_endpoint(r_end), scan_buf, &nvec);

    for (auto itr = scan_buf.begin(); itr != scan_buf.end(); ++itr) {
        write_set_obj* inws = ti->search_write_set((*itr->first)->get_tuple().get_key());
        if (inws != nullptr) {
            /**
             * If the record was already update/insert in the same transaction,
             * the result which is record pointer is notified to caller but
             * don't execute re-read (read_record function).
             * Because in herbrand semantics, the read reads last update even if the
             * update is own.
             */
            if (inws->get_op() == OP_TYPE::DELETE) {
                /**
                 * This transaction deleted this record, so this scan does not include this record in range.
                 */
                continue;
            }
            if (inws->get_op() == OP_TYPE::UPDATE) {
                result.emplace_back(&inws->get_tuple_to_local());
            } else if (inws->get_op() == OP_TYPE::INSERT) {
                result.emplace_back(&inws->get_tuple_to_db());
            } else {
                SPDLOG_DEBUG("It must not reach this points");
                exit(1);
            }
            continue;
        }

        ti->get_read_set().emplace_back(const_cast<Record*>((*itr->first)));
        if (ti->get_node_set().empty() ||
            std::get<1>(ti->get_node_set().back()) != nvec.at(itr - scan_buf.begin()).second) {
            ti->get_node_set().emplace_back(nvec.at(itr - scan_buf.begin()).first,
                                            nvec.at(itr - scan_buf.begin()).second);
        }

        // pre-verify of phantom problem.
        if (std::get<0>(ti->get_node_set().back()) != std::get<1>(ti->get_node_set().back())->get_stable_version()) {
            cc_silo_variant::abort(token);
            return Status::ERR_PHANTOM;
        }

        Status rr = read_record(ti->get_read_set().back().get_rec_read(), const_cast<Record*>((*itr->first)));
        if (rr != Status::OK) {
            if (rset_init_size != ti->get_read_set().size()) {
                ti->get_read_set().erase(ti->get_read_set().begin(),
                                         ti->get_read_set().begin() + (rset_init_size - ti->get_read_set().size()));
            }
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
