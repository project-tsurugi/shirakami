
#include <xmmintrin.h>

#include "clock.h"
#include "storage.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

#include "index/yakushima/include/interface.h"

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
        epoch::epoch_t min_step_epoch{epoch::max_epoch};  // for occ
        epoch::epoch_t min_batch_epoch{epoch::max_epoch}; // for ltx
        auto ce{epoch::get_global_epoch()};
        for (auto&& se : session_table::get_session_table()) {
            if (se.get_visible() && se.get_tx_began()) {
                min_step_epoch = std::min(min_step_epoch, se.get_begin_epoch());
                if (se.get_tx_type() !=
                    transaction_options::transaction_type::SHORT) {
                    min_batch_epoch =
                            std::min(min_batch_epoch, se.get_valid_epoch());
                }
            }
        }
        if (min_step_epoch != epoch::max_epoch) {
            if (min_step_epoch < 1) { LOG(ERROR) << "programming error"; }
            set_min_step_epoch(min_step_epoch);
        } else {
            set_min_step_epoch(ce);
        }
        if (min_batch_epoch != epoch::max_epoch) {
            if (min_batch_epoch_ < 1) { LOG(ERROR) << "programming error"; }
            set_min_batch_epoch(min_batch_epoch);
        } else {
            set_min_batch_epoch(ce + 1);
        }

        sleepMs(PARAM_EPOCH_TIME);
    }
}

version* find_latest_invisible_version_from_batch(Record* rec_ptr,
                                                  version*& pre_ver) {
    version* ver{rec_ptr->get_latest()};
    if (ver == nullptr) {
        // assert. unreachable path
        LOG(ERROR) << "programming error";
    }
    for (;;) {
        ver = ver->get_next();
        if (ver == nullptr) { return nullptr; }
        if (ver->get_tid().get_epoch() < garbage::get_min_batch_epoch()) {
            // hit. ver may be accessed yet.
            pre_ver = ver;
            return ver->get_next();
        }
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
    rec_ptr->get_tidw_ref().lock();
    // reload ts
    check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));

    // ====================
    // main check after lock
    // check after w lock
    rc = check_unhooking_key_ts(check);
    if (rc != Status::OK) { return rc; }

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
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    // register
    auto& cont = garbage::get_container_rec();
    cont.emplace_back(std::make_pair(rec_ptr, epoch::get_global_epoch()));

    // unlock
    rec_ptr->get_tidw_ref().unlock();

    return Status::OK;
}

void unhooking_keys_and_pruning_versions(yakushima::Token ytk, Storage st,
                                         Record* rec_ptr) {
    // unhooking keys
    auto rc{unhooking_key(ytk, st, rec_ptr)};
    if (rc == Status::OK) {
        // unhooked the key.
        return;
    }
    if (rc == Status::ERR_FATAL) {
        LOG(ERROR) << "programming error";
        return;
    }

    version* pre_ver{};
    version* ver{find_latest_invisible_version_from_batch(rec_ptr, pre_ver)};
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
    }
    if (ver != nullptr) {
        // pruning versions
        pre_ver->set_next(nullptr);
        delete_version_list(ver);
    }
}

inline void unhooking_keys_and_pruning_versions(Storage st) {
    std::string_view st_view = {reinterpret_cast<char*>(&st), // NOLINT
                                sizeof(st)};
    // full scan
    yakushima::Token ytk{};
    while (yakushima::enter(ytk) != yakushima::status::OK) { _mm_pause(); }
    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    yakushima::scan(st_view, "", yakushima::scan_endpoint::INF, "",
                    yakushima::scan_endpoint::INF, scan_res);
    for (auto&& sr : scan_res) {
        unhooking_keys_and_pruning_versions(ytk, st, *std::get<1>(sr));
        if (get_flag_cleaner_end()) { break; }
    }
    yakushima::leave(ytk);
}

inline void unhooking_keys_and_pruning_versions() {
    std::vector<Storage> st_list;
    storage::list_storage(st_list);
    for (auto&& st : st_list) {
        if (wp::get_page_set_meta_storage() != st) {
            unhooking_keys_and_pruning_versions(st);
        }
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
    auto ce = epoch::get_global_epoch();
    for (auto itr = cont.begin(); itr != cont.end();) {
        if ((*itr).second < ce) {
            delete (*itr).first; // NOLINT
            itr = cont.erase(itr);
        } else {
            break;
        }
    }
}

void work_cleaner() {
    while (!get_flag_cleaner_end()) {
        {
            std::unique_lock lk{get_mtx_cleaner()};
            unhooking_keys_and_pruning_versions();
            release_key_memory();
        }
        sleepMs(PARAM_EPOCH_TIME);
    }

    force_release_key_memory();
}

} // namespace shirakami::garbage