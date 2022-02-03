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

    std::get<session::scan_handler::scan_cache_storage_pos>(
            ti->get_scan_cache()[handle]) = storage;
    auto& vec = std::get<session::scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    vec.reserve(scan_res.size());
    for (std::size_t i = 0; i < scan_res.size(); ++i) {
        vec.emplace_back(*std::get<index_rec_ptr>(scan_res.at(i)),
                         std::get<index_nvec_body>(nvec.at(i + nvec_delta)),
                         std::get<index_nvec_ptr>(nvec.at(i + nvec_delta)));
    }

    return Status::OK;
}

Status read_from_scan(Token token, ScanHandle handle, // NOLINT
                      Tuple*& tuple) {
    auto* ti = static_cast<session*>(token);
    if (ti->get_read_only()) {
        return snapshot_interface::read_from_scan(ti, handle, tuple);
    }

    /**
     * Check whether the handle is valid.
     */
    if (ti->get_scan_cache().find(handle) == ti->get_scan_cache().end()) {
        return Status::WARN_INVALID_HANDLE;
    }

    auto& scan_buf = std::get<session::scan_handler::scan_cache_vec_pos>(
            ti->get_scan_cache()[handle]);
    std::size_t& scan_index = ti->get_scan_cache_itr()[handle];
retry_by_continue:
    if (scan_buf.size() == scan_index) { return Status::WARN_SCAN_LIMIT; }

    auto itr = scan_buf.begin() + scan_index;

    // check whether it is deleted
    tid_word target_tid{loadAcquire(std::get<0>(*itr)->get_tidw().get_obj())};
    if (!target_tid.get_latest() && target_tid.get_absent()) {
        /**
          * You can skip it with deleted record.
          * The transactional scan logic is as follows.
          * 1: Scan the index.
          * 2: Transactional read the result of scanning the index.
          * 3: Make sure that the transactional read has not been overwritten.
          * 4: Check the node related to scanning for insertion / deletion to prevent phantom problem.
          * If you observe the deleted record at point 2, you don't need to read it.
          * As of 4, it should still be a deleted record.
          * In other words, it suffices if there is no change in the node due to unhooking or the like.
          * If delete interrupted between 1 and 2, 4 can detect it and abort.
          */
        ++scan_index;
        goto retry_by_continue; // NOLINT
    }

#if PARAM_READ_SET_CONT == 1
    // check local read set
    auto rsitr = ti->get_read_set().find(
            const_cast<Record*>(std::get<0>(*itr))); // NOLINT
    if (rsitr != ti->get_read_set().end()) {
        tuple = &(*rsitr).second.get_rec_read().get_tuple();
        return Status::OK;
    }
#endif

    /**
     * Check read-own-write
     */
    const write_set_obj* inws = ti->get_write_set().search(
            const_cast<Record*>(std::get<0>(*itr))); // NOLINT
    if (inws != nullptr) {
        ++scan_index;
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

    Storage storage{std::get<session::scan_handler::scan_cache_storage_pos>(
            ti->get_scan_cache()[handle])};
    read_set_obj rsob(storage, std::get<0>(*itr));

    Status rr = read_record(rsob.get_rec_read(),
                            const_cast<Record*>(std::get<0>(*itr)));
    if (rr != Status::OK) { return rr; }
#if PARAM_READ_SET_CONT == 0
    ti->get_read_set().emplace_back(std::move(rsob));
    tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
#elif PARAM_READ_SET_CONT == 1
    auto pr = ti->get_read_set().insert(std::make_pair(
            const_cast<Record*>(std::get<0>(*itr)), std::move(rsob)));
    tuple = &(*pr.first).second.get_rec_read().get_tuple();
#endif
    ++scan_index;

    // create node set info
    auto& ns = ti->get_node_set();
    if (ns.empty() || std::get<1>(ns.back()) != std::get<2>(*itr)) {
        ns.emplace_back(std::get<1>(*itr), std::get<2>(*itr));
    }

    return Status::OK;
}

Status scan_key(Token token, Storage storage, const std::string_view l_key,
                const scan_endpoint l_end, // NOLINT
                const std::string_view r_key, const scan_endpoint r_end,
                std::vector<const Tuple*>& result, std::size_t const max_size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::scan_key(ti, storage, l_key, l_end, r_key,
                                            r_end, result, max_size);
    }

    // as a precaution
    result.clear();

    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_buf;
    constexpr std::size_t scan_buf_rec_ptr{1};
    std::vector<std::pair<yakushima::node_version64_body,
                          yakushima::node_version64*>>
            nvec;
    yakushima::scan(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            l_key, parse_scan_endpoint(l_end), r_key,
            parse_scan_endpoint(r_end), scan_buf, &nvec, max_size);

    std::int64_t index_ctr{-1};

    std::vector<read_set_obj> rsobs;
    rsobs.reserve(scan_buf.size());
    for (auto&& elem : scan_buf) {
        ++index_ctr;

        // check whether it is deleted
        tid_word target_tid{loadAcquire(
                (*std::get<scan_buf_rec_ptr>(elem))->get_tidw().get_obj())};
        if (!target_tid.get_latest() && target_tid.get_absent()) {
            /**
             * You can skip it with deleted record.
             * The transactional scan logic is as follows.
             * 1: Scan the index.
             * 2: Transactional read the result of scanning the index.
             * 3: Make sure that the transactional read has not been overwritten.
             * 4: Check the node related to scanning for insertion / deletion to prevent phantom problem.
             * If you observe the deleted record at point 2, you don't need to read it.
             * As of 4, it should still be a deleted record.
             * In other words, it suffices if there is no change in the node due to unhooking or the like.
             * If delete interrupted between 1 and 2, 4 can detect it and abort.
             */
            continue;
        }

        // Check local write set.
        write_set_obj* inws = ti->get_write_set().search(
                *std::get<scan_buf_rec_ptr>(elem)); // NOLINT
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
                LOG(FATAL) << "It must not reach this points";
            }
            continue;
        }

#if PARAM_READ_SET_CONT == 1
        // check local read set
        auto rsitr = ti->get_read_set().find(*std::get<scan_buf_rec_ptr>(elem));
        if (rsitr != ti->get_read_set().end()) {
            result.emplace_back(&(*rsitr).second.get_rec_read().get_tuple());
            continue;
        }
#endif


        read_set_obj rs_ob(
                storage,
                const_cast<Record*>((*std::get<scan_buf_rec_ptr>(elem))));
        Status rr = read_record(
                rs_ob.get_rec_read(),
                const_cast<Record*>(*std::get<scan_buf_rec_ptr>(elem)));
        if (rr != Status::OK) {
            // cancel this scan.
            return rr;
        }
        rsobs.emplace_back(std::move(rs_ob));
    }


// success this scana
// reserve rset
#if PARAM_READ_SET_CONT == 0
    ti->get_read_set().reserve(ti->get_read_set().size() + rsobs.size());
#endif
    for (auto&& elem : rsobs) {
#if PARAM_READ_SET_CONT == 0
        ti->get_read_set().emplace_back(std::move(elem));
        result.emplace_back(
                &ti->get_read_set().back().get_rec_read().get_tuple());
#elif PARAM_READ_SET_CONT == 1
        auto pr = ti->get_read_set().insert(std::make_pair(
                const_cast<Record*>(elem.get_rec_ptr()), std::move(elem)));
        result.emplace_back(&(*pr.first).second.get_rec_read().get_tuple());
#endif
    }

    // create node set info
    auto itr_rs = std::unique(nvec.begin(), nvec.end());
    nvec.erase(itr_rs, nvec.end());
    auto& ns = ti->get_node_set();
    ns.reserve(ns.size() + nvec.size());
    ns.insert(ns.end(), nvec.begin(), nvec.end());

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

    size = std::get<session::scan_handler::scan_cache_vec_pos>(
                   ti->get_scan_cache()[handle])
                   .size();
    return Status::OK;
}

} // namespace shirakami
