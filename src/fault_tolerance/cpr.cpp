//
// Created by thawk on 2020/10/30.
//

#include "concurrency_control/silo_variant/include/interface_helper.h"
#include "concurrency_control/silo_variant/include/session_info_table.h"

#include "fault_tolerance/include/log.h"
#include "fault_tolerance/include/cpr.h"

#include "clock.h"

#include "kvs/interface.h"

using namespace shirakami::cc_silo_variant;
using namespace shirakami::cc_silo_variant::epoch;

namespace shirakami::cpr {

void checkpoint_thread() {
    auto wait_worker = [](phase new_phase) {
        bool continue_loop{};
        do {
            continue_loop = false;
            for (auto &&elem : session_info_table::get_thread_info_table()) {
                if (elem.get_visible() && elem.get_phase() == new_phase) {
                    continue;
                }
                continue_loop = true;
                break;
            }
        } while (continue_loop);
    };

    while (likely(kCheckPointThreadEnd.load(std::memory_order_acquire))) {
        sleepMs(PARAM_CHECKPOINT_REST_EPOCH);

        /**
         * PrepareToInProg() phase.
         * Originally, there are 4 phase : rest, prepare, in_progress, wait_flush.
         * But in shirakami, it removes prepare phase to improve performance.
         */
        cpr::global_phase_version::set_gp(cpr::phase::IN_PROGRESS);
        wait_worker(phase::IN_PROGRESS);

        // InProgToWaitFlush() phase
        cpr::global_phase_version::set_gp(cpr::phase::WAIT_FLUSH);
        checkpointing();

        // Atomically set global phase (rest) and increment version.
        cpr::global_phase_version::set_rest();
    }
}

void checkpointing() {
    /**
     * todo
     * 1. If checkpointing is in progress, Deleted record after logical consistency point  must be observable from this
     * thread.
     */
#ifdef INDEX_KOHLER_MASSTREE
    /**
     * todo : impl for kohler masstree
     */
#elif INDEX_YAKUSHIMA
    std::vector<std::pair<Record**, std::size_t>> scan_buf;
    yakushima::scan({}, yakushima::scan_endpoint::INF, {}, yakushima::scan_endpoint::INF, scan_buf); // NOLINT

    phase_version pv = global_phase_version::get_gpv();
    log_records l_recs{};

    std::string checkpointing_path = Log::get_kLogDirectory();
    checkpointing_path += "/checkpointing";
    std::string checkpoint_path = Log::get_kLogDirectory();
    checkpoint_path += "/checkpoint";
    std::ofstream logf;
    logf.open(checkpointing_path, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    if (!logf.is_open()) {
        std::cerr << __FILE__ << " : " << __LINE__ << " : fatal error." << std::endl;
        exit(1);
    }

    yakushima::Token yaku_token{};
    yakushima::enter(yaku_token);
    std::vector<Record*> garbage{};
    for (auto &&itr : scan_buf) {
        Record* rec = *itr.first;
        rec->get_tidw().lock();
        const Tuple &tup = rec->get_stable();
        if (rec->get_version() == pv.get_version() + 1) {
            l_recs.emplace_back(tup.get_key(), tup.get_value());
        } else {
            rec->set_version(rec->get_version() + 1);
            l_recs.emplace_back(tup.get_key(), tup.get_value());
        }
        if (!rec->get_tidw().get_latest()) {
            /**
             * This record was deleted by operation, but deletion is postponed due to not arriving checkpointer.
             */
            yakushima::remove(yaku_token, tup.get_key());
            tid_word new_tid = rec->get_tidw();
            new_tid.set_epoch(kGlobalEpoch.load(std::memory_order_acquire));
            storeRelease(rec->get_tidw().get_obj(), new_tid.get_obj());
            garbage.emplace_back(rec);
        }
        rec->get_tidw().unlock();
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

    msgpack::pack(logf, l_recs);
    logf.flush();
    logf.close();
    if (logf.is_open()) {
        std::cerr << __FILE__ << " : " << __LINE__ << " : fatal error." << std::endl;
        exit(1);
    }

    try {
        boost::filesystem::rename(checkpointing_path, checkpoint_path);
    } catch (boost::filesystem::filesystem_error &ex) {
        std::cout << ex.what() << std::endl;
        exit(1);
    }
#endif

}

}