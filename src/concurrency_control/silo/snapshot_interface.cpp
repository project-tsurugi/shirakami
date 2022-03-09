//
// Created by thawk on 2021/01/19.
//

#include "include/snapshot_interface.h"
#include "include/snapshot_manager.h"

#include "index/yakushima/include/interface.h"
#include "index/yakushima/include/scheme.h"

#include "shirakami/interface.h"
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
    auto rc{scan(storage, l_key, l_end, r_key, r_end, max_size, scan_res,
                 nullptr)};
    if (rc != Status::OK) { return rc; }

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
    Record* rec_ptr{};
    auto rc{get<Record>(storage, key, rec_ptr)};
    if (rc != Status::OK) { return rc; }

    std::string dummy{};
    return read_record(ti, rec_ptr, dummy, false);
}

Status lookup_snapshot(session* ti, Storage storage, std::string_view key,
                       std::string& value) { // NOLINT
    Record* rec_ptr{};
    auto rc{get<Record>(storage, key, rec_ptr)};
    if (rc != Status::OK) { return rc; }

    return read_record(ti, rec_ptr, value);
}

extern Status read_key_from_scan([[maybe_unused]] session* ti,
                                 [[maybe_unused]] ScanHandle handle,
                                 [[maybe_unused]] std::string& key) {
    auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];

    if (scan_buf.size() == scan_index) { return Status::WARN_SCAN_LIMIT; }

    auto itr = scan_buf.begin() + scan_index;
    Record* rec_ptr{const_cast<Record*>(std::get<0>(*itr))};
    rec_ptr->get_key(key);
    std::string dummy{};
    return read_record(ti, rec_ptr, dummy, false);
}

extern Status read_value_from_scan([[maybe_unused]] session* ti,
                                   [[maybe_unused]] ScanHandle handle,
                                   [[maybe_unused]] std::string& value) {
    auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];

    if (scan_buf.size() == scan_index) { return Status::WARN_SCAN_LIMIT; }

    auto itr = scan_buf.begin() + scan_index;
    Record* rec_ptr{const_cast<Record*>(std::get<0>(*itr))};
    return read_record(ti, rec_ptr, value);
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
            if (read_value) { rec_ptr->get_tuple().get_value(value); }
            if (tid == loadAcquire(rec_ptr->get_tidw().get_obj())) {
                // success atomic read
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

} // namespace shirakami::snapshot_interface
