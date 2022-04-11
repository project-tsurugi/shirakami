/**
 * @file scan.cpp
 * @detail implement about scan operation.
 */

#include <map>

#include <glog/logging.h>

#include "include/helper.h"

#include "concurrency_control/silo/include/snapshot_interface.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"
#include "index/yakushima/include/scheme.h"

#include "shirakami/interface.h"


namespace shirakami {

Status close_scan(Token const token, ScanHandle const handle) { // NOLINT
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    return ti->get_scan_handle().clear(handle);
}

inline Status find_open_scan_slot(session* const ti, ScanHandle& out) {
    auto& sh = ti->get_scan_handle();
    for (ScanHandle i = 0;; ++i) {
        auto itr = sh.get_scan_cache().find(i);
        if (itr == sh.get_scan_cache().end()) {
            out = i;
            // clear cursor info
            sh.get_scan_cache_itr()[i] = 0;
            return Status::OK;
        }
    }
    return Status::WARN_SCAN_LIMIT;
}

Status check_not_found(
        session* ti,
        std::vector<std::tuple<std::string, Record**, std::size_t>>& scan_res,
        std::size_t& head_deleted_records) {
    head_deleted_records = 0;
    bool once_not_deleted{false};
    for (auto& elem : scan_res) {
        Record* rec_ptr{*std::get<1>(elem)};
        tid_word tid{loadAcquire(rec_ptr->get_tidw().get_obj())};
        if (!tid.get_absent()) { return Status::OK; }
        if (tid.get_latest()) {
            once_not_deleted = true;
            // inserting page.
            //check read own write
            write_set_obj* inws = ti->get_write_set().search(rec_ptr);
            if (inws != nullptr) {
                if (inws->get_op() == OP_TYPE::INSERT) { return Status::OK; }
                // else: other tx is inserting the page.
            }
        } else {
            if (!once_not_deleted) { ++head_deleted_records; }
        }
    }

    return Status::WARN_NOT_FOUND;
}

Status open_scan(Token const token, Storage storage,
                 const std::string_view l_key, // NOLINT
                 const scan_endpoint l_end, const std::string_view r_key,
                 const scan_endpoint r_end, ScanHandle& handle,
                 std::size_t const max_size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::open_scan(ti, storage, l_key, l_end, r_key,
                                             r_end, handle, max_size);
    }

    auto rc{find_open_scan_slot(ti, handle)};
    if (rc != Status::OK) { return rc; }

    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    constexpr std::size_t index_rec_ptr{1};
    std::vector<std::pair<yakushima::node_version64_body,
                          yakushima::node_version64*>>
            nvec;
    rc = scan(storage, l_key, l_end, r_key, r_end, max_size, scan_res, &nvec);
    if (rc != Status::OK) { return rc; }
    // not empty

    std::size_t head_deleted_recs{};
    rc = check_not_found(ti, scan_res, head_deleted_recs);
    if (rc != Status::OK) { return rc; }

    constexpr std::size_t index_nvec_body{0};
    constexpr std::size_t index_nvec_ptr{1};

    /**
     * You must ensure that new elements are not interrupted in the range at 
     * the node that did not retrieve the element but scanned it when masstree 
     * scanned it.
     */
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

    // Cache a pointer to record.
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

    // increment for head deleted records
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
    scan_index += head_deleted_recs;
    return Status::OK;
}

Status next(Token token, ScanHandle handle) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }
    // valid handle

    // increment cursor
    for (;;) {
        std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
        ++scan_index;
        // check range of cursor
        if (std::get<scan_handler::scan_cache_vec_pos>(
                    ti->get_scan_cache()[handle])
                    .size() <= scan_index) {
            return Status::WARN_SCAN_LIMIT;
        }

        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                ti->get_scan_cache()[handle]);
        auto itr = scan_buf.begin() + scan_index;
        Record* rec_ptr{const_cast<Record*>(std::get<0>(*itr))};

        // check local write set
        const write_set_obj* inws =
                ti->get_write_set().search(rec_ptr); // NOLINT
        if (inws != nullptr) {
            /**
             * If it exists, read from scan api call should be able to read the
             * record.
             */
            break;
        }
        // not in local write set

        tid_word tid{loadAcquire(rec_ptr->get_tidw().get_obj())};
        if (!tid.get_absent()) { break; }
        if (tid.get_latest()) { break; }
    }

    // reset cache in cursor
    ti->get_scan_handle().get_ci(handle).reset();
    return Status::OK;
}

Status read_key_from_scan(Token const token, ScanHandle const handle,
                          std::string& key) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

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
        inws->get_key(key);
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    if (ti->get_scan_handle().get_ci(handle).get_was_read(
                cursor_info::op_type::key)) {
        // it already read.
        ti->get_scan_handle().get_ci(handle).get_key(key);
        return Status::OK;
    }

    tid_word tidb{};
    std::string valueb{};
    const_cast<Record*>(std::get<0>(*itr))->get_key(key);
    Status rr = read_record(const_cast<Record*>(std::get<0>(*itr)), tidb,
                            valueb, false);
    if (rr != Status::OK) { return rr; }
    ti->get_read_set().emplace_back(tidb, std::get<0>(*itr));
    ti->get_scan_handle().get_ci(handle).set_key(key);
    ti->get_scan_handle().get_ci(handle).set_was_read(
            cursor_info::op_type::key);

    // create node set info
    auto& ns = ti->get_node_set();
    if (ns.empty() || std::get<1>(ns.back()) != std::get<2>(*itr)) {
        ns.emplace_back(std::get<1>(*itr), std::get<2>(*itr));
    }

    return Status::OK;
}

Status read_value_from_scan(Token const token, ScanHandle const handle,
                            std::string& value) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

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
        inws->get_value(value);
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    if (ti->get_scan_handle().get_ci(handle).get_was_read(
                cursor_info::op_type::value)) {
        // it already read.
        ti->get_scan_handle().get_ci(handle).get_value(value);
        return Status::OK;
    }

    tid_word tidb{};
    Status rr =
            read_record(const_cast<Record*>(std::get<0>(*itr)), tidb, value);
    if (rr != Status::OK) { return rr; }
    ti->get_read_set().emplace_back(tidb, std::get<0>(*itr));
    ti->get_scan_handle().get_ci(handle).set_value(value);
    ti->get_scan_handle().get_ci(handle).set_was_read(
            cursor_info::op_type::value);

    // create node set info
    auto& ns = ti->get_node_set();
    if (ns.empty() || std::get<1>(ns.back()) != std::get<2>(*itr)) {
        ns.emplace_back(std::get<1>(*itr), std::get<2>(*itr));
    }

    return Status::OK;
}

[[maybe_unused]] Status scannable_total_index_size(Token const token, // NOLINT
                                                   ScanHandle const handle,
                                                   std::size_t& size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

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
