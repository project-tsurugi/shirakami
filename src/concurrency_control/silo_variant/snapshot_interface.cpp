//
// Created by thawk on 2021/01/19.
//

#include "kvs/tuple.h"

#include "concurrency_control/silo_variant/include/snapshot_interface.h"

#include "index/yakushima/include/scheme.h"

using namespace shirakami;
using namespace cc_silo_variant;

namespace shirakami::cc_silo_variant::snapshot_interface {

extern Status
open_scan(Token token, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, // NOLINT
          scan_endpoint r_end, ScanHandle &handle) {
    auto* ti = static_cast<session_info*>(token);

    std::vector<std::pair<Record**, std::size_t>> scan_res;
    yakushima::scan(l_key, parse_scan_endpoint(l_end), r_key, parse_scan_endpoint(r_end), scan_res); // NOLINT
    std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>> scan_buf;
    scan_buf.reserve(scan_res.size());
    for (auto &elem : scan_res) {
        scan_buf.emplace_back(*elem.first, yakushima::node_version64_body{}, nullptr);
    }

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

Status lookup_snapshot(Token token, std::string_view key, Tuple** const ret_tuple) { // NOLINT
    auto* ti = static_cast<session_info*>(token);

    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(key))};
    if (rec_d_ptr == nullptr) {
        // There is no record which has the key.
        *ret_tuple = nullptr;
        return Status::WARN_NOT_FOUND;
    }

    return read_record(ti, *rec_d_ptr, ret_tuple);
}

extern Status read_from_scan(Token token, const ScanHandle handle, Tuple** const tuple) { // NOLINT
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
        const Tuple* tuple_ptr(&std::get<0>(scan_buf.back())->get_tuple());

        std::vector<std::pair<Record**, std::size_t>> scan_res;
        yakushima::scan(tuple_ptr->get_key(), parse_scan_endpoint(scan_endpoint::EXCLUSIVE), // NOLINT
                        ti->get_r_key()[handle], parse_scan_endpoint(ti->get_r_end()[handle]), scan_res);
        std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>> new_scan_buf;
        new_scan_buf.reserve(scan_res.size());
        for (auto &&elem : scan_res) {
            new_scan_buf.emplace_back(*elem.first, yakushima::node_version64_body{}, nullptr);
        }

        if (!new_scan_buf.empty()) {
            // scan could find any records.
            scan_buf.assign(new_scan_buf.begin(), new_scan_buf.end());
            scan_index = 0;
        } else {
            // scan couldn't find any records.
            return Status::WARN_SCAN_LIMIT;
        }
    }

    auto itr = scan_buf.begin() + scan_index;
    return read_record(ti, const_cast<Record*>(std::get<0>(*itr)), tuple);
}

extern Status read_record(session_info* const ti, Record* const rec_ptr, Tuple** const tuple) { // NOLINT
    tid_word tid{};

    // phase 1 : decide to see main record or snapshot.
    for (;;) {
        // phase 1-1 : wait releasing lock
        for (tid = loadAcquire(rec_ptr->get_tidw().get_obj()); tid.get_lock(); _mm_pause(), tid = loadAcquire(
                rec_ptr->get_tidw().get_obj())) {}

        // phase 1-2 : check snapshot epoch
        if (epoch::get_snap_epoch(ti->get_epoch()) > epoch::get_snap_epoch(tid.get_epoch())) {
            /**
             * it should read from main record.
             * If it reads from main record, the read value may change after the reading due to the same memory address.
             * So this case should escape the value to other memory address.
             */
            if (tid.get_absent()) return Status::WARN_NOT_FOUND;
            Tuple escape_tuple = rec_ptr->get_tuple();
            if (tid == loadAcquire(rec_ptr->get_tidw().get_obj())) {
                // success atomic read
                ti->get_read_only_tuples().emplace_back(std::move(escape_tuple));
                *tuple = &ti->get_read_only_tuples().back();
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
    for (Record* snap_ptr = rec_ptr->get_snap_ptr(); snap_ptr != nullptr; snap_ptr = snap_ptr->get_snap_ptr()) {
        if (epoch::get_snap_epoch(ti->get_epoch()) > epoch::get_snap_epoch(snap_ptr->get_tidw().get_epoch())) {
            *tuple = &snap_ptr->get_tuple();
            return Status::OK;
        }
    }
    return Status::WARN_NOT_FOUND; // snap_ptr == nullptr
}

Status
scan_key(Token token, const std::string_view l_key, const scan_endpoint l_end, const std::string_view r_key, // NOLINT
         const scan_endpoint r_end, std::vector<const Tuple*> &result) {
    auto* ti = static_cast<session_info*>(token);
    // as a precaution
    result.clear();

    std::vector<std::pair<Record**, std::size_t>> scan_buf;
    yakushima::scan(l_key, parse_scan_endpoint(l_end), r_key, parse_scan_endpoint(r_end), scan_buf); // NOLINT

    if (scan_buf.empty()) return Status::WARN_NOT_FOUND;
    for (auto &&elem : scan_buf) {
        Record* rec_ptr = *elem.first;
        tid_word tid{};
        // phase 1 : decide to see main record or snapshot.
        bool cont_next_scan_buf{false};
        for (;;) {
            // phase 1-1 : wait releasing lock
            for (tid = loadAcquire(rec_ptr->get_tidw().get_obj()); tid.get_lock(); _mm_pause(), tid = loadAcquire(
                    rec_ptr->get_tidw().get_obj())) {}

            // phase 1-2 : check snapshot epoch
            if (epoch::get_snap_epoch(ti->get_epoch()) > epoch::get_snap_epoch(tid.get_epoch())) {
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
                    ti->get_read_only_tuples().emplace_back(std::move(escape_tuple));
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
        for (Record* snap_ptr = rec_ptr->get_snap_ptr(); snap_ptr != nullptr; snap_ptr = snap_ptr->get_snap_ptr()) {
            if (epoch::get_snap_epoch(ti->get_epoch()) > epoch::get_snap_epoch(snap_ptr->get_tidw().get_epoch())) {
                result.emplace_back(&snap_ptr->get_tuple());
                break;
            }
        }
    }
    return Status::OK;
}

}
