/**
 * @file interface_scan.cpp
 * @detail implement about scan operation.
 */

#include <map>

#include "concurrency_control/include/interface_helper.h"
#include "concurrency_control/include/snapshot_interface.h"

#include "index/yakushima/include/scheme.h"

#include "shirakami/interface.h"

#include "logger.h"
#include "tuple_local.h" // sizeof(Tuple)

using namespace shirakami::logger;

namespace shirakami {

Status close_scan(Token token, ScanHandle handle) { // NOLINT
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

Status open_scan(Token token, Storage storage, const std::string_view l_key, // NOLINT
                 const scan_endpoint l_end, const std::string_view r_key,
                 const scan_endpoint r_end, ScanHandle& handle) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::open_scan(ti, storage, l_key, l_end, r_key, r_end, handle);
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

    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    constexpr std::size_t index_rec_ptr{1};
    std::vector<std::pair<yakushima::node_version64_body, yakushima::node_version64*>> nvec;
    constexpr std::size_t index_nvec_body{0};
    constexpr std::size_t index_nvec_ptr{1};
    yakushima::scan({reinterpret_cast<char*>(&storage), sizeof(storage)}, l_key, parse_scan_endpoint(l_end), r_key, parse_scan_endpoint(r_end), scan_res, &nvec); // NOLINT
    if (scan_res.empty()) {
        /**
         * scan couldn't find any records.
         */
        return Status::WARN_NOT_FOUND;
    }

    ti->get_scan_cache()[handle].reserve(scan_res.size());
    for (std::size_t i = 0; i < scan_res.size(); ++i) {
        ti->get_scan_cache()[handle].emplace_back(*std::get<index_rec_ptr>(scan_res.at(i)), std::get<index_nvec_body>(nvec.at(i)), std::get<index_nvec_ptr>(nvec.at(i)));
    }

    return Status::OK;
}

Status read_from_scan(Token token, ScanHandle handle, // NOLINT
                      Tuple** const tuple) {
    auto* ti = static_cast<session_info*>(token);
    if (ti->get_read_only()) {
        return snapshot_interface::read_from_scan(ti, handle, tuple);
    }

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }

    std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>>& scan_buf = ti->get_scan_cache()[handle];
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
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
        abort(token);
        return Status::ERR_PHANTOM;
    }

    Status rr = read_record(rsob.get_rec_read(), std::get<0>(*itr));
    if (rr != Status::OK) return rr;
    ti->get_read_set().emplace_back(std::move(rsob));
    *tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
    ++scan_index;

    return Status::OK;
}

Status scan_key(Token token, Storage storage, const std::string_view l_key, const scan_endpoint l_end, // NOLINT
                const std::string_view r_key, const scan_endpoint r_end, std::vector<const Tuple*>& result) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::scan_key(ti, storage, l_key, l_end, r_key, r_end, result);
    }

    // as a precaution
    result.clear();
    auto read_set_init_size{ti->get_read_set().size()};
    auto node_set_init_size{ti->get_node_set().size()};

    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_buf;
    constexpr std::size_t scan_buf_rec_ptr{1};
    std::vector<std::pair<yakushima::node_version64_body, yakushima::node_version64*>> nvec;
    yakushima::scan({reinterpret_cast<char*>(&storage), sizeof(storage)}, l_key, parse_scan_endpoint(l_end), r_key, parse_scan_endpoint(r_end), scan_buf, &nvec); // NOLINT

    std::int64_t index_ctr{-1};
    for (auto&& elem : scan_buf) {
        ++index_ctr;
        write_set_obj* inws = ti->search_write_set((*std::get<scan_buf_rec_ptr>(elem))->get_tuple().get_key());
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
                shirakami_logger->debug("It must not reach this points");
                exit(1);
            }
            continue;
        }

        ti->get_read_set().emplace_back(const_cast<Record*>((*std::get<scan_buf_rec_ptr>(elem))));
        if (ti->get_node_set().empty() ||
            std::get<1>(ti->get_node_set().back()) != nvec.at(index_ctr).second) {
            ti->get_node_set().emplace_back(nvec.at(index_ctr).first,
                                            nvec.at(index_ctr).second);
        }

        // pre-verify of phantom problem.
        if (std::get<0>(ti->get_node_set().back()) != std::get<1>(ti->get_node_set().back())->get_stable_version()) {
            abort(token);
            return Status::ERR_PHANTOM;
        }

        Status rr = read_record(ti->get_read_set().back().get_rec_read(), const_cast<Record*>(*std::get<scan_buf_rec_ptr>(elem)));
        if (rr != Status::OK) {
            // cancel this scan.
            if (read_set_init_size != ti->get_read_set().size()) {
                ti->get_read_set().erase(ti->get_read_set().begin(),
                                         ti->get_read_set().begin() + (read_set_init_size - ti->get_read_set().size()));
            }
            if (node_set_init_size != ti->get_node_set().size()) {
                ti->get_node_set().erase(ti->get_node_set().begin(),
                                         ti->get_node_set().begin() + (node_set_init_size - ti->get_node_set().size()));
            }
            return rr;
        }
    }

    if (read_set_init_size != ti->get_read_set().size()) {
        for (auto itr = ti->get_read_set().begin() + read_set_init_size;
             itr != ti->get_read_set().end(); ++itr) {
            result.emplace_back(&itr->get_rec_read().get_tuple());
        }
    }

    return Status::OK;
}

[[maybe_unused]] Status scannable_total_index_size(Token token, // NOLINT
                                                   ScanHandle handle,
                                                   std::size_t& size) {
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

} // namespace shirakami