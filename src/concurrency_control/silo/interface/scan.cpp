/**
 * @file scan.cpp
 * @detail implement about scan operation.
 */

#include <map>

#include <glog/logging.h>

#include "include/helper.h"

#include "concurrency_control/silo/include/snapshot_interface.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/scheme.h"

#include "shirakami/interface.h"


namespace shirakami {

Status close_scan(Token token, ScanHandle handle) { // NOLINT
    auto* ti = static_cast<session*>(token);

    auto itr = ti->get_scan_cache().find(handle);
    if (itr == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }
    ti->get_scan_cache().erase(itr);
    auto index_itr = ti->get_scan_cache_itr().find(handle);
    ti->get_scan_cache_itr().erase(index_itr);

    return Status::OK;
}

Status open_scan(Token token, Storage storage,
                 const std::string_view l_key, // NOLINT
                 const scan_endpoint l_end, const std::string_view r_key,
                 const scan_endpoint r_end, ScanHandle& handle,
                 std::size_t const max_size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::open_scan(ti, storage, l_key, l_end, r_key,
                                             r_end, handle, max_size);
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
    std::vector<std::pair<yakushima::node_version64_body,
                          yakushima::node_version64*>>
            nvec;
    constexpr std::size_t index_nvec_body{0};
    constexpr std::size_t index_nvec_ptr{1};
    yakushima::scan(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            l_key, parse_scan_endpoint(l_end), r_key,
            parse_scan_endpoint(r_end), scan_res, &nvec, max_size);
    if (scan_res.empty()) {
        /**
         * scan couldn't find any records.
         */
        return Status::WARN_NOT_FOUND;
    }

    std::size_t nvec_delta{0};
    if (scan_res.size() < nvec.size()) {
        auto add_ns = [&ti, &nvec](std::size_t n) {
            for (std::size_t i = 0; i < n; ++i) {
                ti->get_node_set().emplace_back(nvec.at(i));
            }
        };
        if (scan_res.size() + 1 == nvec.size()) {
            nvec_delta = 1;
            add_ns(1);

        } else if (scan_res.size() + 2 == nvec.size()) {
            nvec_delta = 2;
            add_ns(2);
        }
    }

    std::get<scan_handler::scan_cache_storage_pos>(
            ti->get_scan_cache()[handle]) = storage;
    auto& vec = std::get<scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    vec.reserve(scan_res.size());
    for (std::size_t i = 0; i < scan_res.size(); ++i) {
        vec.emplace_back(*std::get<index_rec_ptr>(scan_res.at(i)),
                         std::get<index_nvec_body>(nvec.at(i + nvec_delta)),
                         std::get<index_nvec_ptr>(nvec.at(i + nvec_delta)));
    }

    return Status::OK;
}

Status next(Token token, ScanHandle handle) {
    auto* ti = static_cast<session*>(token);
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
    ++scan_index;
    ti->get_scan_handle().get_ci().reset();
    return Status::OK;
}

Status read_key_from_scan(Token token, ScanHandle handle, std::string& key) {
    auto* ti = static_cast<session*>(token);

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }

    if (ti->get_read_only()) {
        std::string os{};
        auto rc{snapshot_interface::read_key_from_scan(ti, handle, os)};
        if (rc != Status::OK) { return rc; }
        key = os;
        return Status::OK;
    }

    scan_handler::scan_elem_type target_elem;
    auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
    auto itr = scan_buf.begin() + scan_index;
    if (scan_buf.size() <= scan_index) { return Status::WARN_SCAN_LIMIT; }

    /**
     * Check read-own-write
     */
    const write_set_obj* inws = ti->get_write_set().search(
            const_cast<Record*>(std::get<0>(*itr))); // NOLINT
    if (inws != nullptr) {
        if (inws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        if (inws->get_op() == OP_TYPE::UPDATE) {
            const_cast<Tuple*>(&inws->get_tuple_to_local())->get_key(key);
        } else {
            // insert/delete
            const_cast<Tuple*>(&inws->get_tuple_to_db())->get_key(key);
        }
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    if (ti->get_scan_handle().get_ci().get_was_read(
                cursor_info::op_type::key)) {
        // it already read.
        ti->get_scan_handle().get_ci().get_key(key);
        return Status::OK;
    }

    tid_word tidb{};
    std::string keyb{};
    std::string valueb{};
    Status rr = read_record(const_cast<Record*>(std::get<0>(*itr)), tidb, keyb,
                            valueb, false);
    if (rr != Status::OK) { return rr; }
    Storage storage{std::get<scan_handler::scan_cache_storage_pos>(
            ti->get_scan_cache()[handle])};
    read_set_obj rsob(storage, std::get<0>(*itr));
    rsob.get_rec_read().set_tidw(tidb);
    rsob.get_rec_read().get_tuple().get_pimpl()->set_key(keyb);
    ti->get_read_set().emplace_back(std::move(rsob));
    key = keyb;
    ti->get_scan_handle().get_ci().set_key(keyb);
    ti->get_scan_handle().get_ci().set_was_read(cursor_info::op_type::key);

    // create node set info
    auto& ns = ti->get_node_set();
    if (ns.empty() || std::get<1>(ns.back()) != std::get<2>(*itr)) {
        ns.emplace_back(std::get<1>(*itr), std::get<2>(*itr));
    }

    return Status::OK;
}

Status read_value_from_scan(Token token, ScanHandle handle,
                            std::string& value) {
    auto* ti = static_cast<session*>(token);

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }

    if (ti->get_read_only()) {
        std::string os{};
        auto rc{snapshot_interface::read_value_from_scan(ti, handle, os)};
        if (rc != Status::OK) { return rc; }
        value = os;
        return Status::OK;
    }

    scan_handler::scan_elem_type target_elem;
    auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
    auto itr = scan_buf.begin() + scan_index;
    if (scan_buf.size() <= scan_index) { return Status::WARN_SCAN_LIMIT; }

    /**
     * Check read-own-write
     */
    const write_set_obj* inws = ti->get_write_set().search(
            const_cast<Record*>(std::get<0>(*itr))); // NOLINT
    if (inws != nullptr) {
        if (inws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        if (inws->get_op() == OP_TYPE::UPDATE) {
            const_cast<Tuple*>(&inws->get_tuple_to_local())->get_value(value);
        } else {
            // insert/delete
            const_cast<Tuple*>(&inws->get_tuple_to_db())->get_value(value);
        }
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    if (ti->get_scan_handle().get_ci().get_was_read(
                cursor_info::op_type::value)) {
        // it already read.
        ti->get_scan_handle().get_ci().get_value(value);
        return Status::OK;
    }

    tid_word tidb{};
    std::string keyb{};
    std::string valueb{};
    Status rr = read_record(const_cast<Record*>(std::get<0>(*itr)), tidb, keyb,
                            valueb);
    if (rr != Status::OK) { return rr; }
    Storage storage{std::get<scan_handler::scan_cache_storage_pos>(
            ti->get_scan_cache()[handle])};
    read_set_obj rsob(storage, std::get<0>(*itr));
    rsob.get_rec_read().set_tidw(tidb);
    rsob.get_rec_read().get_tuple().get_pimpl()->set_value(valueb);
    ti->get_read_set().emplace_back(std::move(rsob));
    value = valueb;
    ti->get_scan_handle().get_ci().set_value(valueb);
    ti->get_scan_handle().get_ci().set_was_read(cursor_info::op_type::value);

    // create node set info
    auto& ns = ti->get_node_set();
    if (ns.empty() || std::get<1>(ns.back()) != std::get<2>(*itr)) {
        ns.emplace_back(std::get<1>(*itr), std::get<2>(*itr));
    }

    return Status::OK;
}

Status read_from_scan(Token token, ScanHandle handle, // NOLINT
                      Tuple*& tuple) {
    auto* ti = static_cast<session*>(token);
    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }

    if (ti->get_read_only()) {
        ti->get_read_only_tuples().emplace_back("", "");
        std::string os{};
        auto rc{snapshot_interface::read_key_from_scan(ti, handle, os)};
        if (rc != Status::OK) {
            next(token, handle);
            return rc;
        }
        ti->get_read_only_tuples().back().get_pimpl()->set_key(os);
        rc = snapshot_interface::read_value_from_scan(ti, handle, os);
        if (rc != Status::OK) {
            next(token, handle);
            return rc;
        }
        ti->get_read_only_tuples().back().get_pimpl()->set_value(os);
        tuple = &ti->get_read_only_tuples().back();
        next(token, handle);
        return Status::OK;
    }

    scan_handler::scan_elem_type target_elem;
    auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
    auto itr = scan_buf.begin() + scan_index;
    if (scan_buf.size() <= scan_index) { return Status::WARN_SCAN_LIMIT; }

    /**
     * Check read-own-write
     */
    const write_set_obj* inws = ti->get_write_set().search(
            const_cast<Record*>(std::get<0>(*itr))); // NOLINT
    if (inws != nullptr) {
        next(token, handle);
        if (inws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        if (inws->get_op() == OP_TYPE::UPDATE) {
            tuple = const_cast<Tuple*>(&inws->get_tuple_to_local());
        } else {
            // insert/delete
            tuple = const_cast<Tuple*>(&inws->get_tuple_to_db());
        }
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    tid_word tidb{};
    std::string keyb{};
    std::string valueb{};
    Status rr = read_record(const_cast<Record*>(std::get<0>(*itr)), tidb, keyb,
                            valueb);
    if (rr != Status::OK) {
        next(token, handle);
        return rr;
    }
    Storage storage{std::get<scan_handler::scan_cache_storage_pos>(
            ti->get_scan_cache()[handle])};
    read_set_obj rsob(storage, std::get<0>(*itr));
    rsob.get_rec_read().set_tidw(tidb);
    rsob.get_rec_read().get_tuple().get_pimpl()->set_key(keyb);
    rsob.get_rec_read().get_tuple().get_pimpl()->set_value(valueb);
    ti->get_read_set().emplace_back(std::move(rsob));
    tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
    ti->get_scan_handle().get_ci().set_key(keyb);
    ti->get_scan_handle().get_ci().set_value(valueb);
    ti->get_scan_handle().get_ci().set_was_read(cursor_info::op_type::key);
    ti->get_scan_handle().get_ci().set_was_read(cursor_info::op_type::value);
    next(token, handle);

    // create node set info
    auto& ns = ti->get_node_set();
    if (ns.empty() || std::get<1>(ns.back()) != std::get<2>(*itr)) {
        ns.emplace_back(std::get<1>(*itr), std::get<2>(*itr));
    }

    return Status::OK;
}

[[maybe_unused]] Status scannable_total_index_size(Token token, // NOLINT
                                                   ScanHandle handle,
                                                   std::size_t& size) {
    auto* ti = static_cast<session*>(token);

    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        /**
         * the handle was invalid.
         */
        return Status::WARN_INVALID_HANDLE;
    }

    size = std::get<scan_handler::scan_cache_vec_pos>(
                   ti->get_scan_cache()[handle])
                   .size();
    return Status::OK;
}

} // namespace shirakami
