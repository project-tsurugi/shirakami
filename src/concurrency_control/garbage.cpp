#include <algorithm>
#include <sstream>
#include <xmmintrin.h>

#include "clock.h"
#include "storage.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "database/include/logging.h"

#ifdef PWAL

#include "datastore/limestone/include/datastore.h"
#include "datastore/limestone/include/limestone_api_helper.h"

#endif

#include "index/yakushima/include/interface.h"

#include "shirakami/logging.h"

#include "yakushima/include/kvs.h"

// third_party

#include "glog/logging.h"

#include "nlohmann/json.hpp"

namespace shirakami::garbage {

static void set_envflags() {
    // check environ "SHIRAKAMI_REDUCE_GC"
    constexpr bool reduce_gc_default = true;
    bool reduce_gc = reduce_gc_default;
    if (auto* envstr = std::getenv("SHIRAKAMI_REDUCE_GC");
        envstr != nullptr && *envstr != '\0') {
        if (std::strcmp(envstr, "1") == 0) {
            reduce_gc = true;
        } else if (std::strcmp(envstr, "0") == 0) {
            reduce_gc = false;
        } else {
            LOG(INFO) << log_location_prefix << "invalid value is set for "
                      << "SHIRAKAMI_REDUCE_GC; using default value";
        }
    }
    VLOG(log_debug) << log_location_prefix << "envflag: reduce_GC is "
                    << (reduce_gc ? "enabled" : "disabled")
                    << (reduce_gc == reduce_gc_default ? " (default)" : "");
    envflag_reduce_gc_ = reduce_gc;
}

void init() {
    set_envflags();
    // output information needed for estimation of memory usage
    VLOG(log_info_gc_stats) << log_location_prefix_detail_info
                            << "sizeof(Record): " << sizeof(Record)
                            << ", sizeof(version): " << sizeof(version);
    // clear global flags
    set_flag_manager_end(false);
    set_flag_cleaner_end(false);

    // clear global statistical data
    get_gc_ct_ver().store(0, std::memory_order_release);

    // initialize timestamps
    set_min_begin_epoch(epoch::initial_epoch);
    set_min_batch_epoch(epoch::initial_epoch);

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
        epoch::epoch_t min_begin_epoch{epoch::max_epoch}; // for occ
        // computing about short
        epoch::epoch_t before_loop{epoch::get_global_epoch()};
        epoch::epoch_t valid_epoch{0};
        for (auto&& se : session_table::get_session_table()) {
            if (se.get_visible() && se.get_tx_began()) {
                min_begin_epoch = std::min(min_begin_epoch, se.get_begin_epoch());
                auto ve = se.get_valid_epoch();
                if (ve != 0) {
                    if (valid_epoch == 0) {
                        valid_epoch = ve;
                    } else {
                        valid_epoch = std::min(valid_epoch, ve);
                    }
                }
            }
        }
        if (min_begin_epoch != epoch::max_epoch) {
            // find some living tx
            if (min_begin_epoch < epoch::initial_epoch) {
                LOG_FIRST_N(ERROR, 1) << log_location_prefix << "epoch error";
            }
        } else {
            /**
             * above loop didn't find living tx. at least, befor_loop epoch is
             * minimum begin epoch.
             */
            min_begin_epoch = before_loop;
        }
        // NB. The calculation method used in the code above has a potential concurrency flaws,
        // as it can miss recently begun transactions,
        // which can result in a smaller value than the previously calculated value of min_begin_epoch.
        // But even if the calculated value of min_begin_epoch is small,
        // OCC reads/writes the latest version, and never reads/writes that version.
        // Therefore, instead of correcting the calculation method, simply discards the smaller value.
        if (auto old = get_min_begin_epoch(); min_begin_epoch > old) {
            set_min_begin_epoch(min_begin_epoch);
        } else if (min_begin_epoch < old) {
            VLOG(log_debug) << log_location_prefix << "min_begin_epoch back from " << old << " to " << min_begin_epoch << " (ignored)";
        }
        // computing about ltx
        if (valid_epoch != 0) {
            // exist some ltx
            auto csse = epoch::get_cc_safe_ss_epoch();
            set_min_batch_epoch(std::min(csse, valid_epoch));
        } else {
            set_min_batch_epoch(epoch::get_cc_safe_ss_epoch());
        }
#ifdef PWAL
        switch_available_boundary_version(shirakami::datastore::get_datastore(), std::min(get_min_begin_epoch(), get_min_batch_epoch()));
#endif

        sleepUs(epoch::get_global_epoch_time_us());
    }
}

static version* find_latest_invisible_version_from_batch(
        Record* rec_ptr, version*& pre_ver,
        stats_info_entry& stats_entry, bool& old_version_still_exists) {
    version* ver{rec_ptr->get_latest()};
    if (ver == nullptr) {
        // assert. unreachable path
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return nullptr;
    }
    // gathering stats info
    std::string val{};
    ver->get_value(val);
    stats_entry.value.accumulate(val);
    for (;;) {
        ver = ver->get_next();
        if (ver == nullptr) { return nullptr; }
        if (ver->get_tid().get_epoch() < garbage::get_min_batch_epoch()) {
            // gathering stats info
            ver->get_value(val);
            stats_entry.value.accumulate(val);
            // hit. ver may be accessed yet.
            pre_ver = ver;
            return ver->get_next();
        }
        old_version_still_exists = true;
        // gathering stats info
        ver->get_value(val);
        stats_entry.value.accumulate(val);
    }
}

static void delete_version_list(version* ver) {
    while (ver != nullptr) {
        version* v_next = ver->get_next();
        delete ver; // NOLINT
        ++get_gc_ct_ver();
        ver = v_next;
    }
}

static Status check_unhooking_key_state(tid_word check) {
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
static inline Status check_unhooking_key_ts(tid_word check) {
    if (
            // threshold for stx.
            check.get_epoch() < garbage::get_min_begin_epoch() &&
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
 * @param[out] absent_still_exists set true if this record is absent but not unhooked
 * @return Status::OK unhooked key
 * @return Status::INTERNAL_WARN_CONCURRENT_INSERT the key is inserted
 * concurrently.
 * @return Status::INTERNAL_WARN_NOT_DELETED the key is not deleted.
 */
static inline Status unhooking_key(yakushima::Token ytk, Storage st, Record* rec_ptr, bool& absent_still_exists) {
    tid_word check{};

    check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));
    // ====================
    // check before lock for reducing lock
    // check before w lock
    Status rc{};
    rc = check_unhooking_key_state(check);
    if (rc != Status::OK) { return rc; } // not absent record

    // check timestamp whether it can unhook.
    rc = check_unhooking_key_ts(check);
    if (rc != Status::OK) { absent_still_exists = true; return rc; } // not old enough
    // ====================

    // w lock
    rec_ptr->get_tidw_ref().lock(true);
    // reload ts
    check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));

    // ====================
    // main check after lock
    // check after w lock
    rc = check_unhooking_key_state(check);
    if (rc != Status::OK) { // not absent record
        rec_ptr->get_tidw_ref().unlock();
        return rc;
    }

    rc = check_unhooking_key_ts(check);
    if (rc != Status::OK) { // not old enough
        rec_ptr->get_tidw_ref().unlock();
        absent_still_exists = true;
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
    cont.emplace_back(rec_ptr, epoch::get_global_epoch());

    if (rec_ptr->get_shared_tombstone_count() != 0) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix
                << "unhooked Record has non-zero shared_tombstone_count";
        VLOG(log_debug) << log_location_prefix
                << "Record: " << rec_ptr << ", shared_tombstone_count: "
                << rec_ptr->get_shared_tombstone_count();
    }

    // unlock
    rec_ptr->get_tidw_ref().unlock();

    return Status::OK;
}

static void unhooking_keys_and_pruning_versions(
        yakushima::Token ytk, Storage st, Record* rec_ptr,
        stats_info_entry& stats_entry, bool& not_collected_record) {
    // unhooking keys
    auto rc{unhooking_key(ytk, st, rec_ptr, not_collected_record)};
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
            rec_ptr, pre_ver, stats_entry, not_collected_record)};
    if (ver == nullptr) {
        // no version from long tx view.
        return;
    }
    // Some occ maybe reads the payload of version.
    for (;;) {
        std::string val{};
        if ((ver->get_tid().get_epoch() <= get_min_begin_epoch())) {
            // gathering stats info
            ver->get_value(val);
            stats_entry.value.accumulate(val);
            // ver can be watched yet
            pre_ver = ver;
            ver = ver->get_next();
            break;
        }
        pre_ver = ver;
        ver = ver->get_next();
        if (ver == nullptr) { return; }
        // gathering stats info
        ver->get_value(val);
        stats_entry.value.accumulate(val);
    }
    if (ver != nullptr) {
        // pruning versions
        pre_ver->set_next(nullptr);
        delete_version_list(ver);
    }
}

static inline void unhooking_keys_and_pruning_versions_at_the_storage(
        Storage st, stats_info_entry& stats_entry) {
    std::string_view st_view = {reinterpret_cast<char*>(&st), // NOLINT
                                sizeof(st)};
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

    bool uncollected_record_exists{false};
    for (auto&& sr : scan_res) {
        Record* rec_ptr = reinterpret_cast<Record*>(std::get<1>(sr)); // NOLINT

        // gathering stats info
        stats_entry.key.accumulate(*rec_ptr->get_key_ptr());
        unhooking_keys_and_pruning_versions(
                ytk, st, rec_ptr, stats_entry, uncollected_record_exists); // NOLINT
        if (get_flag_cleaner_end()) {
            break;
        }
    }
    if (uncollected_record_exists) { // not skip next time
        set_dirty(st);
    }

    // cleanup
    yakushima::leave(ytk);
}

static inline void unhooking_keys_and_pruning_versions(stats_info_type& stats_info) {
    std::vector<Storage> st_list;
    storage::list_storage(st_list);
    for (auto&& st : st_list) {
        if (envflag_reduce_gc_) {
            wp::page_set_meta* psm{};
            auto rc = wp::find_page_set_meta(st, psm);
            if (rc != Status::OK) {
                if (storage::exist_storage(st) == Status::OK) {
                    LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unexpected error, Storage " << st
                                          << " exists, but cannot get page_set_meta of it";
                } else {
                    VLOG(log_debug) << log_location_prefix << "Storage " << st << " is not found";
                }
                continue;
            }
            storage_stats* ssp = psm->get_storage_stats_ptr();
            if (!ssp->worth_to_gc.load(std::memory_order_acquire)) {
                continue;
            }
            ssp->worth_to_gc.store(false, std::memory_order_release);
        }
        stats_info_entry stats_entry{};
        stats_entry.storage = st;
        if (wp::get_page_set_meta_storage() != st) {
            unhooking_keys_and_pruning_versions_at_the_storage(st, stats_entry);
        }
        stats_info.emplace_back(stats_entry);
        if (get_flag_cleaner_end()) { break; }
    }
}

static void force_release_key_memory() {
    auto& cont = garbage::get_container_rec();
    for (auto& elem : cont) { delete elem.first; } // NOLINT
    cont.clear();
}

static void release_key_memory() {
    auto& cont = garbage::get_container_rec();
    // compute minimum epoch
    auto me = std::min(garbage::get_min_begin_epoch(), garbage::get_min_batch_epoch());
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

void set_dirty(Storage st) {
    if (!envflag_reduce_gc_) { return; }

    wp::page_set_meta* psm{};
    auto rc = wp::find_page_set_meta(st, psm);
    if (rc != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unexpected error";
        return;
    }
    storage_stats* ssp = psm->get_storage_stats_ptr();
    if (!ssp->worth_to_gc.load(std::memory_order_acquire)) {
        ssp->worth_to_gc.store(true, std::memory_order_release);
    }
}

static void output_gc_stats(stats_info_type const& stats_info) {
    //std::stringstream ss;
    //ss.clear();
    VLOG(log_info_gc_stats)
            << log_location_prefix_detail_info << "===Stats by GC===";
    VLOG(log_info_gc_stats) << log_location_prefix_detail_info
                            << "# storages: " << stats_info.size();

    auto json_strstat = [](const stats_info_entry::string_stat& sst) {
        nlohmann::json js;
        constexpr std::size_t memuse_roundup = 7UL;
        js["num"] = sst.num;
        js["sum_size"] = sst.sum_size;
        js["ext_num"] = sst.ext_num;
        js["sum_ext_size"] = (sst.sum_ext_size + memuse_roundup) & ~memuse_roundup;
        return js;
    };
    for (const auto& elem : stats_info) {
        std::string str_st_key{};
        /**
         * It may be fail if it executes after delete_storage against it.
         */
        storage::key_handle_map_get_key(elem.storage, str_st_key);
        std::string str_yst_key{reinterpret_cast<const char*>(&elem.storage), sizeof(Storage)}; // NOLINT
        nlohmann::json j;
        j["storage_key"] = str_st_key;
        j["yakushima_storage_key"] = str_yst_key;
        j["num_entries"] = elem.key.num;
        if (elem.key.num != 0) {
            j["av_len_ver_list_per_entry"] = static_cast<double>(elem.value.num) / static_cast<double>(elem.key.num);
            j["av_key_size_per_entry"] = static_cast<double>(elem.key.sum_size) / static_cast<double>(elem.key.num);
            if (elem.value.num != 0) {
                j["av_val_size_per_entry"]
                        = static_cast<double>(elem.value.sum_size) / static_cast<double>(elem.value.num);
            }
            j["record_allocated"] = sizeof(Record) * elem.key.num;
            j["version_allocated"] = sizeof(version) * elem.value.num;
            j["keys"] = json_strstat(elem.key);
            j["values"] = json_strstat(elem.value);
        }
        VLOG(log_info_gc_stats) << log_location_prefix_detail_info << j;
    }
}

void work_cleaner() {
    while (!get_flag_cleaner_end()) {
        // prepare for detail info
        /**
         * Storage, number of entry in the storage, average length of version,
         * average length of key, average length of value
         */
        stats_info_type stats_info;
        stats_info.clear();

        // gc
        {
            std::unique_lock lk{get_mtx_cleaner()};
            unhooking_keys_and_pruning_versions(stats_info);
            if (get_flag_cleaner_end()) { break; }
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
