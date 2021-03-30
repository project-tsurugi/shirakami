//
// created by thawk on 2020/10/30.
//

#include "concurrency_control/silo_variant/include/interface_helper.h"
#include "concurrency_control/silo_variant/include/session_info_table.h"
#include "concurrency_control/silo_variant/include/snapshot_manager.h"

#include "fault_tolerance/include/log.h"

#include "clock.h"
#include "logger.h"

#include "kvs/interface.h"

using namespace shirakami::epoch;
using namespace shirakami::logger;

namespace shirakami::cpr {

#ifndef PARAM_CPR_USE_FULL_SCAN

void aggregate_diff_update_set(tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>& aggregate_buf) {
    phase_version pv = global_phase_version::get_gpv();
    auto index{pv.get_version() % 2 == 0 ? 0 : 1};
    for (auto&& table_elem : session_info_table::get_thread_info_table()) {
        auto absorbed_map = table_elem.get_diff_update_set(index);
        for (auto map_elem = absorbed_map.begin(); map_elem != absorbed_map.end(); ++map_elem) {
            if (aggregate_buf[map_elem.key()].first < map_elem.value().first) {
                aggregate_buf[map_elem.key()] = map_elem.value(); // merge
            }
        }
    }
}

tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>& cpr_local_handler::get_diff_update_set() {
    version_type cv{get_version()};
    if ((cv % 2 == 0 && get_phase() == phase::REST) ||
        (cv % 2 == 1 && get_phase() != phase::REST)) {
        return diff_update_set.at(0);
    }
    return diff_update_set.at(1);
}

#endif

void checkpoint_thread() {
    setup_spdlog();
    auto wait_worker = [](phase new_phase) {
        bool continue_loop{}; // NOLINT
        do {
            continue_loop = false;
            for (auto&& elem : session_info_table::get_thread_info_table()) {
                if (elem.get_visible() && elem.get_txbegan() && elem.get_phase() != new_phase) {
                    continue_loop = true;
                    break;
                    _mm_pause();
                }
            }
            if (kCheckPointThreadEnd.load(std::memory_order_acquire)) break;
        } while (continue_loop);
    };

    while (likely(!kCheckPointThreadEnd.load(std::memory_order_acquire))) {
        sleepMs(PARAM_CHECKPOINT_REST_EPOCH);

        /**
         * preparetoinprog() phase.
         * originally, there are 4 phase : rest, prepare, IN_PROGRESS, WAIT_FLUSH.
         * but in shirakami, it removes prepare phase to improve performance.
         */
        cpr::global_phase_version::set_gp(cpr::phase::IN_PROGRESS);
        wait_worker(phase::IN_PROGRESS);
        if (kCheckPointThreadEnd.load(std::memory_order_acquire)) break;

        // inprogtowaitflush() phase
        cpr::global_phase_version::set_gp(cpr::phase::WAIT_FLUSH);
        checkpointing();

        // atomically set global phase (rest) and increment version.
        cpr::global_phase_version::set_rest();
    }
}

void checkpointing() {
#ifdef PARAM_CPR_USE_FULL_SCAN
    std::vector<std::pair<Record**, std::size_t>> scan_buf;
    yakushima::scan({}, yakushima::scan_endpoint::INF, {}, yakushima::scan_endpoint::INF, scan_buf); // NOLINT
#else
    tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>> aggregate_buf;
    aggregate_diff_update_set(aggregate_buf);
#endif

#ifdef PARAM_CPR_USE_FULL_SCAN
    if (scan_buf.empty()) {
#else
    if (aggregate_buf.empty()) {
#endif
        //shirakami_logger->debug("cpr : database has no records. end checkpointing.");
        if (boost::filesystem::exists(get_checkpoint_path())) {
            boost::filesystem::remove(get_checkpoint_path());
        }
        return;
    }

    std::ofstream logf;
    logf.open(get_checkpointing_path(), std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    if (!logf.is_open()) {
        shirakami_logger->debug("it can't open file.");
        exit(1);
    }

    yakushima::Token yaku_token{};
    yakushima::enter(yaku_token);
    std::vector<Record*> garbage{};
    phase_version pv = global_phase_version::get_gpv();
    log_records l_recs{};
    Token shira_token{};
    enter(shira_token);
    /**
     * the snapshot transaction manager aligns the work of removing snapshots from the index and freeing memory with
     * the progress of the epoch.
     * if this thread does not coordinate with it, it can access the freed memory.
     * therefore, by joining this thread to the session and starting a transaction in a pseudo manner, the view of
     * memory is protected.
     */
    tx_begin(shira_token); // NOLINT
#ifdef PARAM_CPR_USE_FULL_SCAN
    for (auto&& itr : scan_buf) {
        Record* rec = *itr.first;
#else
    for (auto&& itr : aggregate_buf) {
        Record* rec = itr.second.second;
#endif
        rec->get_tidw().lock();
        /**
         * you may find it redundant to take a lock. however, if it does not take the lock, it must join this thread in
         * the view protection protocol to avoid segv.
         */
        auto remove_from_index_and_register_garbage = [&]() {
            tid_word new_tid = rec->get_tidw();
            new_tid.set_epoch(kGlobalEpoch.load(std::memory_order_acquire));
            if (rec->get_snap_ptr() == nullptr) {
                // if no snapshot, it can immediately remove.
                yakushima::remove(yaku_token, rec->get_tuple().get_key());
                garbage.emplace_back(rec);
            }
            storeRelease(rec->get_tidw().get_obj(), new_tid.get_obj());
        };
        if (rec->get_failed_insert()) {
            /**
             * this record was inserted and aborted between cpr logical consistency point and scan by this thread,
             * so omit checkpoint processing.
             */
            remove_from_index_and_register_garbage();
            rec->get_tidw().unlock();
        } else {
            if (
                    (
                            /**
                             * do not include records inserted after the checkpoint boundary and before this thread
                             * scans the index.
                             */
                            rec->get_tidw().get_latest() &&
                            rec->get_not_include_version() != -1 &&
                            pv.get_version() != static_cast<uint64_t>(rec->get_not_include_version())) ||
                    (
                            /**
                             * do not include records that were deleted before the checkpoint boundary but were left in
                             * the index for the snapshot transaction.
                             */
                            rec->get_tidw().get_absent() &&
                            !rec->get_tidw().get_latest() &&
                            rec->get_not_include_version() != -1 &&
                            pv.get_version() < static_cast<uint64_t>(rec->get_not_include_version()))) {
                // begin : copy record
                /**
                 * todo : it can be expected to be faster by refining it.
                 * after all, while cooperating with the worker thread to save the value, various processing is
                 * done after acquiring the lock. this is due to various complicated processing and arbitration, but
                 * it can be expected to be faster by refining it.
                 */
                if (rec->get_version() == pv.get_version() + 1) {
                    const Tuple& tup = rec->get_stable();
                    l_recs.emplace_back(tup.get_key(), tup.get_value());
                } else {
                    const Tuple& tup = rec->get_tuple();
                    l_recs.emplace_back(tup.get_key(), tup.get_value());
                    if (rec->get_tidw().get_latest()) {
                        /**
                         * update only the version number to prevent other workers from making redundant copies after
                         * releasing the lock.
                         */
                        rec->set_version(pv.get_version() + 1);
                    }
                }
                // end : copy record
            }
            if (!rec->get_tidw().get_latest()) {
                /**
                 * this record was deleted by operation, but deletion is postponed due to not arriving checkpointer.
                 */
                remove_from_index_and_register_garbage();
            }
            if (rec->get_snap_ptr() == nullptr) {
                rec->get_tidw().unlock();
            } else {
                rec->get_tidw().unlock();
                snapshot_manager::remove_rec_cont.push(rec);
            }
        }
        if (kCheckPointThreadEnd.load(std::memory_order_acquire)) break;
    }
    yakushima::leave(yaku_token);
    commit(shira_token); // NOLINT
    leave(shira_token);

    /**
     * release old garbage
     */
    auto ers_bgn_itr = garbage.begin();
    auto ers_end_itr = garbage.end();
    for (auto itr = ers_bgn_itr; itr != garbage.end(); ++itr) {
        if ((*itr)->get_tidw().get_epoch() <= epoch::get_reclamation_epoch()) {
            ers_end_itr = itr;
            delete *itr; // NOLINT
        } else {
            break;
        }
    }
    if (ers_end_itr != garbage.end()) {
        garbage.erase(ers_bgn_itr, ers_end_itr);
    }

    /**
     * record garbage to some session area.
     */
    enter(shira_token);
    tx_begin(shira_token); // NOLINT
    auto* ti = static_cast<session_info*>(shira_token);
    for (auto&& itr : garbage) {
        tid_word new_tid = itr->get_tidw();
        new_tid.set_epoch(ti->get_epoch());
        storeRelease(itr->get_tidw().get_obj(), new_tid.get_obj());
        ti->get_gc_record_container().emplace_back(itr);
    }
    leave(shira_token);

    if (kCheckPointThreadEnd.load(std::memory_order_acquire)) return;

    /**
     * starting logging and flushing
     */
    msgpack::pack(logf, l_recs);
    logf.flush();
    logf.close();
    if (logf.is_open()) {
        shirakami_logger->debug("it can't close log file.");
        exit(1);
    }

    try {
        boost::filesystem::rename(get_checkpointing_path(), get_checkpoint_path());
    } catch (boost::filesystem::filesystem_error& ex) {
        shirakami_logger->debug("fail rename : {0}.", ex.what());
        exit(1);
    } catch (...) {
        shirakami_logger->debug("fail rename : unknown.");
    }
}

void wait_next_checkpoint() {
    cpr::phase_version pv = cpr::global_phase_version::get_gpv();
    switch (pv.get_phase()) {
        case cpr::phase::REST: // NOLINT
            while (pv.get_version() == cpr::global_phase_version::get_gpv().get_version()) _mm_pause();
            break;
        case cpr::phase::IN_PROGRESS:
        case cpr::phase::WAIT_FLUSH:
            while (pv.get_version() + 2 <= cpr::global_phase_version::get_gpv().get_version()) _mm_pause();
            break;
    }
}

} // namespace shirakami::cpr