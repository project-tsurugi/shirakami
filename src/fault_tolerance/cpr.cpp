//
// created by thawk on 2020/10/30.
//

#include "concurrency_control/include/interface_helper.h"
#include "concurrency_control/include/session_info_table.h"
#include "concurrency_control/include/snapshot_manager.h"

#include "fault_tolerance/include/log.h"

#include "clock.h"
#include "logger.h"

#include "shirakami/interface.h"

using namespace shirakami::epoch;
using namespace shirakami::logger;

namespace shirakami::cpr {

void aggregate_diff_update_set(tsl::hopscotch_map<std::string, tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>>& aggregate_buf) {
    phase_version pv = global_phase_version::get_gpv();
    auto index{pv.get_version() % 2 == 0 ? 0 : 1};
    for (auto&& table_elem : session_info_table::get_thread_info_table()) {
        auto absorbed_set = table_elem.get_diff_update_set(index);
        for (auto absorbed_storage = absorbed_set.begin(); absorbed_storage != absorbed_set.end(); ++absorbed_storage) {
            for (auto map_elem = absorbed_storage.value().begin(); map_elem != absorbed_storage.value().end(); ++map_elem) {
                if ((aggregate_buf.find(absorbed_storage.key()) == aggregate_buf.end()) ||                                                                                                              // not found storage in aggregate_buf
                    (aggregate_buf.find(absorbed_storage.key()) != aggregate_buf.end() && aggregate_buf[absorbed_storage.key()].find(map_elem.key()) != aggregate_buf[absorbed_storage.key()].end()) || // found storage but not found elem in aggregate_buf
                    aggregate_buf[absorbed_storage.key()][map_elem.key()].first < map_elem.value().first) {
                    aggregate_buf[absorbed_storage.key()][map_elem.key()] = map_elem.value(); // merge
                }
            }
            absorbed_storage.value().clear();
        }
        absorbed_set.clear(); // clean up.
    }
    clear_register_count(index);
}

void aggregate_update_sequence_set(tsl::hopscotch_map<SequenceValue, std::tuple<SequenceVersion, SequenceValue>>& aggregate_buf) {
    phase_version pv = global_phase_version::get_gpv();
    auto index{pv.get_version() % 2 == 0 ? 0 : 1};
    for (auto&& table_elem : session_info_table::get_thread_info_table()) {
        auto absorbed_map = table_elem.get_diff_update_sequence_set(index);
        for (auto map_elem = absorbed_map.begin(); map_elem != absorbed_map.end(); ++map_elem) {
            if (aggregate_buf.find(map_elem.key()) == aggregate_buf.end() || std::get<0>(map_elem.value()) > std::get<0>(aggregate_buf[map_elem.key()])) {
                aggregate_buf[map_elem.key()] = map_elem.value();
            }
        }
        absorbed_map.clear(); // clean up.
    }
}

tsl::hopscotch_map<std::string, tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>>& cpr_local_handler::get_diff_update_set() {
    version_type cv{get_version()};
    if ((cv % 2 == 0 && get_phase() == phase::REST) ||
        (cv % 2 == 1 && get_phase() != phase::REST)) {
        return diff_update_set.at(0);
    }
    return diff_update_set.at(1);
}

tsl::hopscotch_map<SequenceValue, std::tuple<SequenceVersion, SequenceValue>>& cpr_local_handler::get_diff_update_sequence_set() {
    version_type cv{get_version()};
    if ((cv % 2 == 0 && get_phase() == phase::REST) ||
        (cv % 2 == 1 && get_phase() != phase::REST)) {
        return diff_update_sequence_set.at(0);
    }
    return diff_update_sequence_set.at(1);
}

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
    tsl::hopscotch_map<std::string, tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>> aggregate_buf;
    aggregate_diff_update_set(aggregate_buf);
    tsl::hopscotch_map<SequenceValue, std::tuple<SequenceVersion, SequenceValue>> aggregate_buf_seq;
    aggregate_update_sequence_set(aggregate_buf_seq);

    std::ofstream logf;
    if (aggregate_buf.size() + aggregate_buf_seq.size() != 0) {
        logf.open(cpr::get_checkpointing_path(), std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        if (!logf.is_open()) {
            shirakami_logger->debug("It can't open file.");
            exit(1);
        }
    } else {
        return; // no update in this epoch.
    }

    log_records l_recs{};
    if (aggregate_buf.size() > 0) {
        yakushima::Token yaku_token{};
        yakushima::enter(yaku_token);
        phase_version pv = global_phase_version::get_gpv();
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
        auto* ti = static_cast<session_info*>(shira_token);
        for (auto itr_storage = aggregate_buf.begin(); itr_storage != aggregate_buf.end(); ++itr_storage) {
            for (auto itr = itr_storage.value().begin(); itr != itr_storage.value().end(); ++itr) {
                Record* rec = itr.value().second;

                if (rec == nullptr) {
                    l_recs.emplace_back(std::string_view(itr.key()));
                    continue;
                }
                rec->get_tidw().lock();
                // begin : copy record
                if (rec->get_version() == pv.get_version()) {
                    const Tuple& tup = rec->get_tuple();
                    l_recs.emplace_back(tup.get_key(), tup.get_value());
                    /**
              * update only the version number to prevent other workers from making 
              * redundant copies after releasing the lock.
              */
                    rec->set_version(pv.get_version() + 1);
                } else if (rec->get_version() == pv.get_version() + 1) {
                    const Tuple& tup = rec->get_stable();
                    l_recs.emplace_back(tup.get_key(), tup.get_value());

                    // for deleted record
                    tid_word c_tid = rec->get_tidw();
                    if (!c_tid.get_latest() && c_tid.get_absent()) {
                        c_tid.set_epoch(ti->get_epoch());
                        storeRelease(rec->get_tidw().get_obj(), c_tid.get_obj());
                        if (rec->get_snap_ptr() == nullptr) {
                            yakushima::remove(yaku_token, itr_storage.key(), rec->get_tuple().get_key());
                            ti->get_gc_record_container().emplace_back(rec);
                        } else {
                            snapshot_manager::remove_rec_cont.push({itr_storage.key(), rec});
                        }
                    }
                } else {
                    shirakami_logger->debug("fatal error");
                    exit(1);
                }
                // end : copy record


                // unlock record
                rec->get_tidw().unlock();
            }
        }

        yakushima::leave(yaku_token);
        leave(shira_token);
    }

    if (aggregate_buf_seq.size() > 0) {
        for (auto&& elem : aggregate_buf_seq) {
            l_recs.emplace_back_seq({std::get<0>(elem), std::get<1>(elem)});
        }
    }

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

    std::string fname{Log::get_kLogDirectory() + "/sst" + std::to_string(global_phase_version::get_gpv().get_version())};
    try {
        boost::filesystem::rename(get_checkpointing_path(), fname);
    } catch (boost::filesystem::filesystem_error& ex) {
        shirakami_logger->debug("filesystem_error.");
        exit(1);
    } catch (...) {
        shirakami_logger->debug("unknown error.");
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
            while (pv.get_version() + 2 <= cpr::global_phase_version::get_gpv().get_version()) _mm_pause();
            break;
    }
}

} // namespace shirakami::cpr