
/**
 * @file snapshot_manager.cpp
 */

#include "clock.h"
#include "compiler.h"
#include "logger.h"

#include "concurrency_control/silo_variant/include/snapshot_manager.h"

#include "yakushima/include/kvs.h"

namespace shirakami::cc_silo_variant::snapshot_manager {

void snapshot_manager_func() {

    while (likely(!snapshot_manager_thread_end.load(std::memory_order_acquire))) {
        sleepMs(1);

        epoch::epoch_t maybe_smallest_ew = epoch::kGlobalEpoch.load(std::memory_order_acquire);
        if (maybe_smallest_ew != 0) --maybe_smallest_ew;
        if (!remove_rec_cont.empty()) {
            yakushima::Token yaku_token{};
            bool yaku_entered{false};
            std::size_t erase_num{0};

            /**
             * TODO : enhancement.
             * This parts (remove_rec_cont_mutex) can be r-w lock, but now mutex lock.
             */
            remove_rec_cont_mutex.lock();
            for (auto &&elem : remove_rec_cont) {
                if (elem->get_snap_ptr() == nullptr) {
                    SPDLOG_DEBUG("fatal error.");
                    exit(1);
                }
                if (epoch::get_snap_epoch(elem->get_snap_ptr()->get_tidw().get_epoch()) !=
                    epoch::get_snap_epoch(maybe_smallest_ew)) {
                    if (!yaku_entered) {
                        yakushima::enter(yaku_token);
                        yaku_entered = true;
                    }
                    yakushima::remove(yaku_token, elem->get_tuple().get_key());
                    ++erase_num;
                    release_rec_cont.emplace_back(std::make_pair(maybe_smallest_ew, elem));
                } else {
                    break;
                }
            }
            if (erase_num != 0) {
                remove_rec_cont.erase(remove_rec_cont.begin(), remove_rec_cont.begin() + erase_num);
            }
            remove_rec_cont_mutex.unlock();

            if (yaku_entered) {
                yakushima::leave(yaku_token);
            }
        }

        if (!release_rec_cont.empty()) {
            std::size_t erase_num{0};
            for (auto &&elem : release_rec_cont) {
                if (elem.first < maybe_smallest_ew) {
                    ++erase_num;
                    delete elem.second; // NOLINT
                } else {
                    break;
                }
            }
            if (erase_num != 0) {
                release_rec_cont.erase(release_rec_cont.begin(), release_rec_cont.begin() + erase_num);
            }
        }
    }
}

} //  namespace shirakami::cc_silo_variant::snapshot_manager