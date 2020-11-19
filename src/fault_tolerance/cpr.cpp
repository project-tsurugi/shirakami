//
// Created by thawk on 2020/10/30.
//

#include "concurrency_control/silo_variant/include/interface_helper.h"
#include "concurrency_control/silo_variant/include/session_info_table.h"

#include "fault_tolerance/include/log.h"
#include "fault_tolerance/include/cpr.h"

#include "clock.h"
#include "logger.h"

#include "kvs/interface.h"

#include "spdlog/spdlog.h"

using namespace shirakami::cc_silo_variant;
using namespace shirakami::cc_silo_variant::epoch;
using namespace shirakami::logger;

namespace shirakami::cpr {

void checkpoint_thread() {
    setup_spdlog();
    SPDLOG_DEBUG("start checkpoint thread.");
    auto wait_worker = [](phase new_phase) {
        bool continue_loop{};
        do {
            continue_loop = false;
            for (auto &&elem : session_info_table::get_thread_info_table()) {
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
         * PrepareToInProg() phase.
         * Originally, there are 4 phase : rest, prepare, in_progress, wait_flush.
         * But in shirakami, it removes prepare phase to improve performance.
         */
        cpr::global_phase_version::set_gp(cpr::phase::IN_PROGRESS);
        wait_worker(phase::IN_PROGRESS);
        if (kCheckPointThreadEnd.load(std::memory_order_acquire)) break;

        // InProgToWaitFlush() phase
        cpr::global_phase_version::set_gp(cpr::phase::WAIT_FLUSH);
        checkpointing();

        // Atomically set global phase (rest) and increment version.
        cpr::global_phase_version::set_rest();
    }
}

void checkpointing() {
#ifdef INDEX_KOHLER_MASSTREE
    /**
     * todo : impl for kohler masstree
     */
#elif INDEX_YAKUSHIMA
    std::vector<std::pair<Record**, std::size_t>> scan_buf;
    yakushima::scan({}, yakushima::scan_endpoint::INF, {}, yakushima::scan_endpoint::INF, scan_buf); // NOLINT

    if (scan_buf.empty()) {
        //SPDLOG_DEBUG("cpr : Database has no records. End checkpointing.");
        if (boost::filesystem::exists(get_checkpoint_path())) {
            boost::filesystem::remove(get_checkpoint_path());
        }
        return;
    }

    std::ofstream logf;
    logf.open(get_checkpointing_path(), std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    if (!logf.is_open()) {
        SPDLOG_DEBUG("It can't open file.");
        exit(1);
    }

    yakushima::Token yaku_token{};
    yakushima::enter(yaku_token);
    std::vector<Record*> garbage{};
    phase_version pv = global_phase_version::get_gpv();
    log_records l_recs{};
    for (auto &&itr : scan_buf) {
        Record* rec = *itr.first;
        rec->get_tidw().lock();
        /**
         * You may find it redundant to take a lock. However, if it does not take the lock, it must join this thread in
         * the view protection protocol to avoid segv.
         */
        if (rec->get_version() == pv.get_version() + 1) {
            const Tuple &tup = rec->get_stable();
            l_recs.emplace_back(tup.get_key(), tup.get_value());
        } else {
            const Tuple &tup = rec->get_tuple();
            l_recs.emplace_back(tup.get_key(), tup.get_value());
            if (rec->get_tidw().get_latest()) {
                /**
                 * If it is still live record, capture stable for partial checkpointing.
                 * Partial checkpointing of cpr doesn't exist at writing this sentence. So todo.
                 */
                if (!(rec->get_tidw().get_epoch() == rec->get_stable_tidw().get_epoch()
                      && rec->get_tidw().get_tid() == rec->get_stable_tidw().get_tid())) {
                    rec->get_stable() = tup;
                    rec->get_stable_tidw() = rec->get_tidw();
                }
                rec->set_version(rec->get_version() + 1);
            }
        }
        if (!rec->get_tidw().get_latest()) {
            /**
             * This record was deleted by operation, but deletion is postponed due to not arriving checkpointer.
             */
            yakushima::remove(yaku_token, rec->get_tuple().get_key());
            tid_word new_tid = rec->get_tidw();
            new_tid.set_epoch(kGlobalEpoch.load(std::memory_order_acquire));
            storeRelease(rec->get_tidw().get_obj(), new_tid.get_obj());
            garbage.emplace_back(rec);
        }
        rec->get_tidw().unlock();
        if (kCheckPointThreadEnd.load(std::memory_order_acquire)) break;
    }
    yakushima::leave(yaku_token);

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
    Token shira_token{};
    enter(shira_token);
    tx_begin(shira_token);
    auto* ti = static_cast<cc_silo_variant::session_info*>(shira_token);
    for (auto &&itr : garbage) {
        tid_word new_tid = itr->get_tidw();
        new_tid.set_epoch(ti->get_epoch());
        storeRelease(itr->get_tidw().get_obj(), new_tid.get_obj());
        ti->get_gc_record_container()->emplace_back(itr);
    }
    leave(shira_token);

    if (kCheckPointThreadEnd.load(std::memory_order_acquire)) return;

    msgpack::pack(logf, l_recs);
    logf.flush();
    logf.close();
    if (logf.is_open()) {
        SPDLOG_DEBUG("It can't close log file.");
        exit(1);
    }

    try {
        boost::filesystem::rename(get_checkpointing_path(), get_checkpoint_path());
    } catch (boost::filesystem::filesystem_error &ex) {
        SPDLOG_DEBUG("Fail rename : {0}.", ex.what());
        exit(1);
    } catch (...) {
        SPDLOG_DEBUG("Fail rename : unknown.");
    }
#endif
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

}