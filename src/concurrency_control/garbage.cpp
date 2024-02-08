#include <sstream>
#include <xmmintrin.h>

#include "clock.h"
#include "storage.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

#include "database/include/logging.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/logging.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami::garbage {

void init() {
    // clear global flags
    set_flag_manager_end(false);
    set_flag_cleaner_end(false);

    // clear global statistical data
    get_gc_ct_ver().store(0, std::memory_order_release);

    invoke_bg_threads();
}

void fin() {
    // set flags
    set_flag_manager_end(true);
    set_flag_cleaner_end(true);

    join_bg_threads();
}

void work_manager() {
    // compute gc timestamp
    while (!get_flag_manager_end()) {
        epoch::epoch_t min_step_epoch{epoch::max_epoch}; // for occ
        // computing about short
        epoch::epoch_t before_loop{epoch::get_global_epoch()};
        epoch::epoch_t valid_epoch{0};
        for (auto&& se : session_table::get_session_table()) {
            if (se.get_visible() && se.get_tx_began()) {
                min_step_epoch = std::min(min_step_epoch, se.get_begin_epoch());
                auto ve = se.get_valid_epoch();
                if (ve != 0) { valid_epoch = ve; }
            }
        }
        if (min_step_epoch != epoch::max_epoch) {
            // find some living tx
            if (min_step_epoch < epoch::initial_epoch) {
                LOG_FIRST_N(ERROR, 1) << log_location_prefix
                                      << log_location_prefix << "epoch error";
            }
            set_min_step_epoch(min_step_epoch);
        } else {
            /**
             * above loop didn't find living tx. at least, befor_loop epoch is
             * minimum step epoch.
            */
            set_min_step_epoch(before_loop);
        }
        // computing about ltx
        if (valid_epoch != 0) {
            // exist some ltx
            auto csse = epoch::get_cc_safe_ss_epoch();
            set_min_batch_epoch(csse < valid_epoch ? csse : valid_epoch);
        } else {
            set_min_batch_epoch(epoch::get_cc_safe_ss_epoch());
        }

        sleepUs(epoch::get_global_epoch_time_us());
    }
}

version* find_latest_invisible_version_from_batch(
        Record* rec_ptr, version*& pre_ver,
        std::size_t& average_version_list_size) {
    version* ver{rec_ptr->get_latest()};
    if (ver == nullptr) {
        // assert. unreachable path
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << log_location_prefix
                              << "unreachable path";
    }
    // gathering stats info
    ++average_version_list_size;
    for (;;) {
        ver = ver->get_next();
        if (ver == nullptr) { return nullptr; }
        if (ver->get_tid().get_epoch() < garbage::get_min_batch_epoch()) {
            // hit. ver may be accessed yet.
            pre_ver = ver;
            return ver->get_next();
        }
        // gathering stats info
        ++average_version_list_size;
    }
}

void delete_version_list(version* ver) {
    while (ver != nullptr) {
        version* v_next = ver->get_next();
        delete ver; // NOLINT
        ++get_gc_ct_ver();
        ver = v_next;
    }
}

Status check_unhooking_key_state(tid_word check) {
    if (check.get_latest() && check.get_absent()) {
        return Status::INTERNAL_WARN_CONCURRENT_INSERT;
    }
    if (!check.get_absent()) { return Status::INTERNAL_WARN_NOT_DELETED; }
    return Status::OK;
}

/**
 * @brief check timestamp of the key whether it can unhook.
 * 
 * @param[in] check 
 * @return Status::OK it can unhook from the point of view of timestamp.
 * @return Status::INTERNAL_WARN_PREMATURE it can't unhook from the point of 
 * view of timestamp.
 */
inline Status check_unhooking_key_ts(tid_word check) {
    if (
            // threshold for stx.
            check.get_epoch() < garbage::get_min_step_epoch() &&
            // this records version is not needed by current and future long tx.
            check.get_epoch() < garbage::get_min_batch_epoch()) {
        return Status::OK;
    }
    return Status::INTERNAL_WARN_PREMATURE;
}

/**
 * @brief check whether it can unhook the key. If check was passed, it 
 * executes unhooking.
 * @param[in] st 
 * @param[in] rec_ptr 
 * @return Status::OK unhooked key
 * @return Status::INTERNAL_WARN_CONCURRENT_INSERT the key is inserted 
 * concurrently.
 * @return Status::INTERNAL_WARN_NOT_DELETED the key is not deleted.
 */
inline Status unhooking_key(yakushima::Token ytk, Storage st, Record* rec_ptr) {
    tid_word check{};

    check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));
    // ====================
    // check before lock for reducing lock
    // check timestamp whether it can unhook.
    auto rc = check_unhooking_key_ts(check);
    if (rc != Status::OK) { return rc; }

    // check before w lock
    rc = check_unhooking_key_state(check);
    if (rc != Status::OK) { return rc; }
    // ====================

    // w lock
    rec_ptr->get_tidw_ref().lock(true);
    // reload ts
    check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));

    // ====================
    // main check after lock
    // check after w lock
    rc = check_unhooking_key_ts(check);
    if (rc != Status::OK) {
        rec_ptr->get_tidw_ref().unlock();
        return rc;
    }

    rc = check_unhooking_key_state(check);
    if (rc != Status::OK) {
        rec_ptr->get_tidw_ref().unlock();
        return rc;
    }
    // ====================

    // unhook and register gc container
    // unhook
    std::string kb{};
    rec_ptr->get_key(kb);
    rc = remove(ytk, st, kb);
    if (rc != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "unreachable path: it can't find the record on yakushima,"
                   "it is unexpected. yakushima return code: "
                << rc;
        return Status::ERR_FATAL;
    }

    // register record and minimum epoch of step or batch.
    auto& cont = garbage::get_container_rec();
    cont.emplace_back(std::make_pair(rec_ptr, epoch::get_global_epoch()));

    // unlock
    rec_ptr->get_tidw_ref().unlock();

    return Status::OK;
}

void unhooking_keys_and_pruning_versions(
        yakushima::Token ytk, Storage st, Record* rec_ptr,
        std::size_t& average_version_list_size) {
    // unhooking keys
    auto rc{unhooking_key(ytk, st, rec_ptr)};
    if (rc == Status::OK) {
        // unhooked the key.
        return;
    }
    if (rc == Status::ERR_FATAL) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "unreachable path: it may be programming error.";
        return;
    }

    version* pre_ver{};
    version* ver{find_latest_invisible_version_from_batch(
            rec_ptr, pre_ver, average_version_list_size)};
    if (ver == nullptr) {
        // no version from long tx view.
        return;
    }
    // Some occ maybe reads the payload of version.
    for (;;) {
        if ((ver->get_tid().get_epoch() <= get_min_step_epoch())) {
            // ver can be watched yet
            pre_ver = ver;
            ver = ver->get_next();
            break;
        }
        pre_ver = ver;
        ver = ver->get_next();
        if (ver == nullptr) { return; }
        // gathering stats info
        ++average_version_list_size;
    }
    if (ver != nullptr) {
        // pruning versions
        pre_ver->set_next(nullptr);
        delete_version_list(ver);
    }
}

inline void unhooking_keys_and_pruning_versions_at_the_storage(
        Storage st, std::size_t& record_num,
        std::size_t& average_version_list_size, std::size_t& average_key_size,
        std::size_t& average_value_size) {
    std::string_view st_view = {reinterpret_cast<char*>(&st), // NOLINT
                                sizeof(st)};
    // init about stats
    record_num = 0;
    average_version_list_size = 0;
    average_key_size = 0;
    average_value_size = 0;

    // full scan
    yakushima::Token ytk{};
    while (yakushima::enter(ytk) != yakushima::status::OK) { _mm_pause(); }
    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    yakushima::scan(st_view, "", yakushima::scan_endpoint::INF, "",
                    yakushima::scan_endpoint::INF, scan_res);
    if (scan_res.empty()) {
        yakushima::leave(ytk);
        return;
    } // empty by current action
    // not empty

    // gathering stats info
    record_num = scan_res.size();

    auto process_before_fin = [record_num, &average_version_list_size,
                               &average_key_size, &average_value_size]() {
        // maybe 0 due to current delete action or 0 length key or value
        if (average_version_list_size != 0) {
            average_version_list_size /= record_num;
        }
        if (average_key_size != 0) { average_key_size /= record_num; }
        if (average_value_size != 0) { average_value_size /= record_num; }
    };

    for (auto&& sr : scan_res) {
        Record* rec_ptr = reinterpret_cast<Record*>(std::get<1>(sr)); // NOLINT

        // gathering stats info
        average_key_size += rec_ptr->get_key_view().size();
        std::string buf;
        rec_ptr->get_value(buf);
        average_value_size += buf.size();

        unhooking_keys_and_pruning_versions(
                ytk, st, rec_ptr, average_version_list_size); // NOLINT
        if (get_flag_cleaner_end()) {
            process_before_fin();
            break;
        }
    }

    // cleanup
    yakushima::leave(ytk);

    process_before_fin();
}

inline void unhooking_keys_and_pruning_versions(stats_info_type& stats_info) {
    std::vector<Storage> st_list;
    storage::list_storage(st_list);
    for (auto&& st : st_list) {
        std::size_t entry_num{};
        std::size_t average_version_list_size{};
        std::size_t average_key_size{};
        std::size_t average_value_size{};
        if (wp::get_page_set_meta_storage() != st) {
            unhooking_keys_and_pruning_versions_at_the_storage(
                    st, entry_num, average_version_list_size, average_key_size,
                    average_value_size);
        }
        stats_info.emplace_back(
                std::make_tuple(st, entry_num, average_version_list_size,
                                average_key_size, average_value_size));
        if (get_flag_cleaner_end()) { break; }
    }
}

void force_release_key_memory() {
    auto& cont = garbage::get_container_rec();
    for (auto& elem : cont) { delete elem.first; } // NOLINT
    cont.clear();
}

void release_key_memory() {
    auto& cont = garbage::get_container_rec();
    // compute minimum epoch
    auto me = garbage::get_min_step_epoch() < garbage::get_min_batch_epoch()
                      ? garbage::get_min_step_epoch()
                      : garbage::get_min_batch_epoch();
    std::size_t erase_count{0};
    for (auto itr = cont.begin(); itr != cont.end();) { // NOLINT
        /**
          * If me changed from unhooking, all tx which existed at unhooking must
          * have finished.
          */
        if ((*itr).second < me) {
            delete (*itr).first; // NOLINT
            ++erase_count;
            ++itr;
        } else {
            break;
        }
    }
    if (erase_count > 0) {
        cont.erase(cont.begin(), cont.begin() + erase_count); // NOLINT
    }
}

void output_gc_stats(stats_info_type const& stats_info) {
    std::stringstream ss;
    ss.clear();
    ss << log_location_prefix_detail_info << "===Stats by GC===" << std::endl
       << "# storages: " << stats_info.size() << std::endl;

    for (const auto& elem : stats_info) {
        std::string str_st_key{};
        /**
         * It may be fail if it executes after delete_storage against it.
        */
        storage::key_handle_map_get_key(std::get<0>(elem), str_st_key);
        ss << "storage key: " << str_st_key << std::endl
           << "# entries: " << std::get<1>(elem) << std::endl
           << "avarage length of version list per entry: " << std::get<2>(elem)
           << std::endl
           << "average key size per entry: " << std::get<3>(elem) << std::endl
           << "average value size per entry: " << std::get<4>(elem)
           << std::endl;
    }
    VLOG(log_trace) << ss.str();
}

void work_cleaner() {
    while (!get_flag_cleaner_end()) {
        // prepare for detail info
        /**
         * Storage, number of entry in the storage, average length of version,
         * average length of key, average length of value
         **/
        stats_info_type stats_info;
        stats_info.clear();

        // gc
        {
            std::unique_lock lk{get_mtx_cleaner()};
            unhooking_keys_and_pruning_versions(stats_info);
            release_key_memory();
        }

        // output detail info
        if (logging::get_enable_logging_detail_info()) {
            // logging detail info
            output_gc_stats(stats_info);
        }

        // sleep
        sleepUs(epoch::get_global_epoch_time_us());
    }
    force_release_key_memory();
}

} // namespace shirakami::garbage