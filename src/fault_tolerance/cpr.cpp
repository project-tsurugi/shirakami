//
// created by thawk on 2020/10/30.
//

#ifdef WP

#include "concurrency_control/wp/include/interface_helper.h"
#include "concurrency_control/wp/include/session_info_table.h"
#include "concurrency_control/wp/include/snapshot_manager.h"

#else

#include "concurrency_control/silo/include/interface_helper.h"
#include "concurrency_control/silo/include/session_info_table.h"
#include "concurrency_control/silo/include/snapshot_manager.h"

#endif

#include "fault_tolerance/include/log.h"

#include "clock.h"

#include "shirakami/interface.h"

using namespace shirakami::epoch;

namespace shirakami::cpr {

void aggregate_diff_upd_set(cpr_local_handler::diff_upd_set_type& aggregate_buf) {
    for (auto&& table_elem : session_info_table::get_thread_info_table()) {
        if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;
        phase_version pv = global_phase_version::get_gpv();
        auto index{pv.get_version() % 2 == 0 ? 0 : 1};
        auto& absorbed_set = table_elem.get_diff_upd_set(index);
        for (auto absorbed_storage = absorbed_set.begin(); absorbed_storage != absorbed_set.end(); ++absorbed_storage) {
            if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;
#if defined(CPR_DIFF_HOPSCOTCH)
            for (auto map_elem = absorbed_storage.value().begin(); map_elem != absorbed_storage.value().end(); ++map_elem) {
                if ((aggregate_buf.find(absorbed_storage.key()) == aggregate_buf.end()) ||                                                                                                              // not found storage in aggregate_buf
                    (aggregate_buf.find(absorbed_storage.key()) != aggregate_buf.end() && aggregate_buf[absorbed_storage.key()].find(map_elem.key()) != aggregate_buf[absorbed_storage.key()].end()) || // found storage but not found elem in aggregate_buf
                    std::get<cpr::cpr_local_handler::diff_timestamp_pos>(aggregate_buf[absorbed_storage.key()][map_elem.key()]) < std::get<cpr_local_handler::diff_timestamp_pos>(map_elem.value())) {
                    aggregate_buf[absorbed_storage.key()][map_elem.key()] = map_elem.value(); // merge
                }
            }
            absorbed_storage.value().clear();
            if (cpr_local_handler::reserve_num != 0) {
                absorbed_storage.value().reserve(cpr_local_handler::reserve_num); // reserve for next
            }
#elif defined(CPR_DIFF_UM)
            for (auto map_elem = absorbed_storage->second.begin(); map_elem != absorbed_storage->second.end(); ++map_elem) {
                if ((aggregate_buf.find(absorbed_storage->first) == aggregate_buf.end()) ||                                                                                                                 // not found storage in aggregate_buf
                    (aggregate_buf.find(absorbed_storage->first) != aggregate_buf.end() && aggregate_buf[absorbed_storage->first].find(map_elem->first) != aggregate_buf[absorbed_storage->first].end()) || // found storage but not found elem in aggregate_buf
                    aggregate_buf[absorbed_storage->first][map_elem->first].first < map_elem->second.first) {
                    aggregate_buf[absorbed_storage->first][map_elem->first] = map_elem->second; // merge
                }
            }
            absorbed_storage->second.clear();
            absorbed_storage->second.reserve(cpr_local_handler::reserve_num); // reserve for next
#endif
        }
        absorbed_set.clear(); // clean up.
        if (cpr_local_handler::reserve_num != 0) {
            absorbed_set.reserve(cpr_local_handler::reserve_num); // reserve for next
        }
    }
}

void aggregate_diff_upd_seq_set(cpr_local_handler::diff_upd_seq_set_type& aggregate_buf) {
    phase_version pv = global_phase_version::get_gpv();
    auto index{pv.get_version() % 2 == 0 ? 0 : 1};
    for (auto&& table_elem : session_info_table::get_thread_info_table()) {
        if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;
        auto& absorbed_map = table_elem.get_diff_upd_seq_set(index);
        for (auto map_elem = absorbed_map.begin(); map_elem != absorbed_map.end(); ++map_elem) {
            if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;
            if (aggregate_buf.find(map_elem.key()) == aggregate_buf.end() || std::get<0>(map_elem.value()) > std::get<0>(aggregate_buf[map_elem.key()])) {
                aggregate_buf[map_elem.key()] = map_elem.value();
            }
        }
        absorbed_map.clear();                                 // clean up.
        absorbed_map.reserve(cpr_local_handler::reserve_num); // reserve for next
    }
}

cpr_local_handler::diff_upd_set_type& cpr_local_handler::get_diff_upd_set() {
    version_type cv{get_version()};
    if ((cv % 2 == 0 && get_phase() == phase::REST) ||
        (cv % 2 == 1 && get_phase() != phase::REST)) {
        return diff_upd_set_ar.at(0);
    }
    return diff_upd_set_ar.at(1);
}

cpr_local_handler::diff_upd_seq_set_type& cpr_local_handler::get_diff_upd_seq_set() {
    version_type cv{get_version()};
    if ((cv % 2 == 0 && get_phase() == phase::REST) ||
        (cv % 2 == 1 && get_phase() != phase::REST)) {
        return diff_upd_seq_set_ar.at(0);
    }
    return diff_upd_seq_set_ar.at(1);
}

void checkpoint_thread() {
    auto wait_worker = [](phase new_phase) {
        bool continue_loop{}; // NOLINT
        do {
            continue_loop = false;
            for (auto&& elem : session_info_table::get_thread_info_table()) {
                if (elem.get_visible() && elem.get_txbegan() && elem.get_phase() != new_phase) {
                    continue_loop = true;
                    _mm_pause();
                    break;
                }
            }
            if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) break;
        } while (continue_loop);
    };

    while (likely(!(get_checkpoint_thread_end() && get_checkpoint_thread_end_force()))) {
        sleepMs(PARAM_CHECKPOINT_REST_EPOCH);
        if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) break;

        /**
         * preparetoinprog() phase.
         * originally, there are 4 phase : rest, prepare, IN_PROGRESS, WAIT_FLUSH.
         * but in shirakami, it removes prepare phase to improve performance.
         */
        cpr::global_phase_version::set_gp(cpr::phase::IN_PROGRESS);
        wait_worker(phase::IN_PROGRESS);

        // inprogtowaitflush() phase
        cpr::global_phase_version::set_gp(cpr::phase::WAIT_FLUSH);
        if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) break;
        checkpointing();
        if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) break;

        // atomically set global phase (rest) and increment version.
        cpr::global_phase_version::set_rest();

        // Termination process that is not forced termination.
        if (get_checkpoint_thread_end() && !get_checkpoint_thread_end_force()) {
            if (session_info_table::is_empty_logs()) {
                break;
            }
            // else: continue to do logging all log records.
        }
    }
}

void checkpointing() {
    cpr_local_handler::diff_upd_set_type aggregate_buf;
    aggregate_diff_upd_set(aggregate_buf);
    if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;
    cpr_local_handler::diff_upd_seq_set_type aggregate_buf_seq;
    aggregate_diff_upd_seq_set(aggregate_buf_seq);
    if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;

    std::ofstream logf;
    if (aggregate_buf.size() + aggregate_buf_seq.size() != 0) {
        logf.open(cpr::get_checkpointing_path(), std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        if (!logf.is_open()) {
            std::cerr << "It can't open file." << std::endl;
            exit(1);
        }
    } else {
        return; // no update in this epoch.
    }

    log_records l_recs{};
    if (!aggregate_buf.empty()) {
        /**
     * the snapshot transaction manager aligns the work of removing snapshots from the index and freeing memory with
     * the progress of the epoch.
     * if this thread does not coordinate with it, it can access the freed memory.
     * therefore, by joining this thread to the session and starting a transaction in a pseudo manner, the view of
     * memory is protected.
     */
        for (auto itr_storage = aggregate_buf.begin(); itr_storage != aggregate_buf.end(); ++itr_storage) {
#if defined(CPR_DIFF_HOPSCOTCH)
            for (auto itr = itr_storage.value().begin(); itr != itr_storage.value().end(); ++itr) {
#elif defined(CPR_DIFF_UM)
            for (auto itr = itr_storage->second.begin(); itr != itr_storage->second.end(); ++itr) {
#endif
                if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;

                if (std::get<cpr_local_handler::diff_is_delete_pos>(itr.value())) {
                    l_recs.emplace_back(itr_storage.key(), std::string_view(itr.key()));
                } else {
                    l_recs.emplace_back(itr_storage.key(), std::string_view(itr.key()),
                                        std::get<cpr_local_handler::diff_value_pos>(itr.value()));
                }
            }
        }
    }

    if (!aggregate_buf_seq.empty()) {
        for (auto&& elem : aggregate_buf_seq) {
            if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;
            l_recs.emplace_back_seq({std::get<0>(elem), std::get<1>(elem)});
        }
    }

    /**
     * starting logging and flushing
     */
    msgpack::pack(logf, l_recs);
    if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;
    logf.flush();
    logf.close();
    if (logf.is_open()) {
        std::cerr << "it can't close log file." << std::endl;
        exit(1);
    }

    if (get_checkpoint_thread_end() && get_checkpoint_thread_end_force()) return;
    std::string fname{Log::get_kLogDirectory() + "/sst" + std::to_string(global_phase_version::get_gpv().get_version())};
    try {
        boost::filesystem::rename(get_checkpointing_path(), fname);
    } catch (boost::filesystem::filesystem_error& ex) {
        std::cerr << "filesystem_error." << std::endl;
        exit(1);
    } catch (...) {
        std::cerr << "unknown error" << std::endl;
        exit(1);
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
            while (pv.get_version() + 2 >= cpr::global_phase_version::get_gpv().get_version()) _mm_pause();
            break;
    }
}

} // namespace shirakami::cpr