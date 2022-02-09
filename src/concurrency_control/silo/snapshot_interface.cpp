//
// Created by thawk on 2021/01/19.
//

#include "include/snapshot_interface.h"
#include "include/snapshot_manager.h"

#include "index/yakushima/include/scheme.h"

#include "shirakami/tuple.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::snapshot_interface {

extern Status open_scan(session* ti, Storage storage, std::string_view l_key,
                        scan_endpoint l_end, std::string_view r_key, // NOLINT
                        scan_endpoint r_end, ScanHandle& handle,
                        std::size_t max_size) {

    for (ScanHandle i = 0;; ++i) {
        auto itr = ti->get_scan_cache().find(i);
        if (itr == ti->get_scan_cache().end()) {
            handle = i;
            ti->get_scan_cache_itr()[handle] = 0;
            break;
        }
        if (i == SIZE_MAX) return Status::WARN_SCAN_LIMIT;
    }

    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    constexpr std::size_t scan_res_rec_ptr{1};
    yakushima::scan(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            l_key, parse_scan_endpoint(l_end), r_key,
            parse_scan_endpoint(r_end), scan_res, nullptr, max_size);
    if (scan_res.empty()) {
        /**
         * scan couldn't find any records.
         */
        return Status::WARN_NOT_FOUND;
    }
    /**
     * scan could find any records.
     */
    auto& vec = std::get<scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    std::get<scan_handler::scan_cache_storage_pos>(
            ti->get_scan_cache()[handle]) = storage;
    for (auto& elem : scan_res) {
        vec.emplace_back(*std::get<scan_res_rec_ptr>(elem),
                         yakushima::node_version64_body{}, nullptr);
    }

    return Status::OK;
}

Status lookup_snapshot(session* ti, Storage storage,
                       std::string_view key) { // NOLINT
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            key))};
    if (rec_d_ptr == nullptr) {
        // There is no record which has the key.
        return Status::WARN_NOT_FOUND;
    }

    std::string dummy{};
    return read_record(ti, *rec_d_ptr, dummy, false);
}

Status lookup_snapshot(session* ti, Storage storage, std::string_view key,
                       std::string& value) { // NOLINT
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            key))};
    if (rec_d_ptr == nullptr) {
        // There is no record which has the key.
        return Status::WARN_NOT_FOUND;
    }

    return read_record(ti, *rec_d_ptr, value);
}

extern Status read_from_scan(session* ti, const ScanHandle handle,
                             Tuple*& tuple) { // NOLINT
    // opt for small memory
    ti->get_read_only_tuples().clear();

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }

    auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];

    for (;;) {
        if (scan_buf.size() == scan_index) { return Status::WARN_SCAN_LIMIT; }

        auto itr = scan_buf.begin() + scan_index;
        ++scan_index;
        ti->get_read_only_tuples().clear();
        Record* rec_ptr{const_cast<Record*>(std::get<0>(*itr))};
        std::string key{};
        rec_ptr->get_key(key);
        ti->get_read_only_tuples().emplace_back(key, "");
        std::string out{};
        auto rc{read_record(ti, rec_ptr, out)};
        if (rc == Status::OK || rc == Status::WARN_CONCURRENT_INSERT) {
            if (rc == Status::OK) {
                ti->get_read_only_tuples().back().get_pimpl()->set_value(out);
                tuple = &ti->get_read_only_tuples().back();
            }
            return rc;
        }
    }
}

extern Status read_record(session* const ti, Record* const rec_ptr,
                          std::string& value, bool read_value) { // NOLINT
    tid_word tid{};

    // phase 1 : decide to see main record or snapshot.
    for (;;) {
        // phase 1-1 : wait releasing lock
        for (tid = loadAcquire(rec_ptr->get_tidw().get_obj()); tid.get_lock();
             _mm_pause(), tid = loadAcquire(rec_ptr->get_tidw().get_obj())) {
            if (tid.get_absent()) return Status::WARN_CONCURRENT_INSERT;
        }

        // phase 1-2 : check snapshot epoch
        if (snapshot_manager::get_snap_epoch(ti->get_epoch()) >
            snapshot_manager::get_snap_epoch(tid.get_epoch())) {
            /**
             * it should read from main record.
             * If it reads from main record, the read value may change after the reading due to the same memory address.
             * So this case should escape the value to other memory address.
             */
            if (tid.get_absent()) return Status::WARN_NOT_FOUND;
            std::string read_value_st;
            if (read_value) { rec_ptr->get_tuple().get_value(read_value_st); }
            if (tid == loadAcquire(rec_ptr->get_tidw().get_obj())) {
                // success atomic read
                if (read_value) {
                    std::string key{};
                    rec_ptr->get_key(key);
                    ti->get_read_only_tuples().emplace_back(key, read_value_st);
                    ti->get_read_only_tuples().back().get_value(value);
                }
                return Status::OK;
            }
            // fail atomic read
            continue;
        }
        break;
    }
    /**
     * It should read from snapshot.
     * Note that snapshot must not be absent (deleted) record.
     * This snap may be appropriate.
     * This read does not need to be verified whether it could be read atomically from the list.
     * Because the position that can be interrupted is the second, it is always from the main version,
     * and this transaction does not have to read it.
     */
    for (Record* snap_ptr = rec_ptr->get_snap_ptr(); snap_ptr != nullptr;
         snap_ptr = snap_ptr->get_snap_ptr()) {
        if (snapshot_manager::get_snap_epoch(ti->get_epoch()) >
            snapshot_manager::get_snap_epoch(
                    snap_ptr->get_tidw().get_epoch())) {
            if (read_value) { snap_ptr->get_tuple().get_value(value); }
            return Status::OK;
        }
    }
    return Status::WARN_NOT_FOUND; // snap_ptr == nullptr
}

Status scan_key(session* ti, Storage storage, const std::string_view l_key,
                const scan_endpoint l_end, // NOLINT
                const std::string_view r_key, const scan_endpoint r_end,
                std::vector<const Tuple*>& result, std::size_t max_size) {
    // as a precaution
    result.clear();

    // optimize for small memory
    ti->get_read_only_tuples().clear();

    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_buf;
    constexpr std::size_t scan_buf_rec_ptr{1};
    yakushima::scan(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            l_key, parse_scan_endpoint(l_end), r_key,
            parse_scan_endpoint(r_end), scan_buf, nullptr, max_size);

    if (scan_buf.empty()) return Status::WARN_NOT_FOUND;
    for (auto&& elem : scan_buf) {
        Record* rec_ptr = *std::get<scan_buf_rec_ptr>(elem);
        tid_word tid{};
        // phase 1 : decide to see main record or snapshot.
        bool cont_next_scan_buf{false};
        for (;;) {
            // phase 1-1 : wait releasing lock
            for (tid = loadAcquire(rec_ptr->get_tidw().get_obj());
                 tid.get_lock(); _mm_pause(),
                tid = loadAcquire(rec_ptr->get_tidw().get_obj())) {}

            // phase 1-2 : check snapshot epoch
            if (snapshot_manager::get_snap_epoch(ti->get_epoch()) >
                snapshot_manager::get_snap_epoch(tid.get_epoch())) {
                /**
                 * it should read from main record.
                 * If it reads from main record, the read value may change after the reading due to the same memory address.
                 * So this case should escape the value to other memory address.
                 */
                if (tid.get_absent()) {
                    cont_next_scan_buf = true;
                    break;
                }
                Tuple escape_tuple = rec_ptr->get_tuple();
                if (tid == loadAcquire(rec_ptr->get_tidw().get_obj())) {
                    // success atomic read
                    ti->get_read_only_tuples().emplace_back(
                            std::move(escape_tuple));
                    result.emplace_back(&ti->get_read_only_tuples().back());
                    cont_next_scan_buf = true;
                    break;
                }
                // fail atomic read
                continue;
            }
            break;
        }
        if (cont_next_scan_buf) continue;
        /**
         * It should read from snapshot.
         * Note that snapshot must not be absent (deleted) record.
         * This snap may be appropriate.
         * This read does not need to be verified whether it could be read atomically from the list.
         * Because the position that can be interrupted is the second, it is always from the main version,
         * and this transaction does not have to read it.
         */
        for (Record* snap_ptr = rec_ptr->get_snap_ptr(); snap_ptr != nullptr;
             snap_ptr = snap_ptr->get_snap_ptr()) {
            if (snapshot_manager::get_snap_epoch(ti->get_epoch()) >
                snapshot_manager::get_snap_epoch(
                        snap_ptr->get_tidw().get_epoch())) {
                result.emplace_back(&snap_ptr->get_tuple());
                break;
            }
        }
    }
    return Status::OK;
}

} // namespace shirakami::snapshot_interface
