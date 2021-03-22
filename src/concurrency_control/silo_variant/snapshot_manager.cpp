
/**
 * @file snapshot_manager.cpp
 */

#include "concurrency_control/silo_variant/include/snapshot_manager.h"

#include "clock.h"
#include "compiler.h"
#include "logger.h"
#include "yakushima/include/kvs.h"

namespace shirakami::snapshot_manager {
void snapshot_manager_func() {
    // Memory used by elements in this container will be released.
    std::vector<std::pair<epoch::epoch_t, Record*>> release_rec_cont;
    /**
     * If the queue does not have a front reference function and it is not the time to process what was retrieved, 
     * it is necessary to carry it over to the next consideration. 
     * In other words, it is necessary when processing according to order in a container that stores ordered elements.
     */
    Record* cache_for_queue{nullptr};

    while (likely(!snapshot_manager_thread_end.load(std::memory_order_acquire))) {
        // todo parametrize  for build options.
        sleepMs(1);

        epoch::epoch_t maybe_smallest_ew = epoch::kGlobalEpoch.load(std::memory_order_acquire);
        if (maybe_smallest_ew != 0) --maybe_smallest_ew;

        yakushima::Token yaku_token{};
        bool yaku_entered{false};
        while (!remove_rec_cont.empty() || cache_for_queue != nullptr) {
            Record* elem{};
            if (cache_for_queue != nullptr) {
                elem = cache_for_queue;
                cache_for_queue = nullptr;
            } else {
                if (!remove_rec_cont.try_pop(elem)) {
                    // Although there is an element in the contents, it failed to take out. Is there a false positive for the act of taking from the container?
                    break;
                }
            }

            if (elem->get_snap_ptr() == nullptr) {
                SPDLOG_DEBUG("fatal error.");
                exit(1);
            }
            if (snapshot_manager::get_snap_epoch(
                    elem->get_snap_ptr()->get_tidw().get_epoch()) !=
                snapshot_manager::get_snap_epoch(
                        maybe_smallest_ew)) {// todo : measures for
                // round-trip of epoch.
                if (!yaku_entered) {
                    yakushima::enter(yaku_token);
                    yaku_entered = true;
                }
                yakushima::remove(yaku_token, elem->get_tuple().get_key());
                release_rec_cont.emplace_back(
                        std::make_pair(maybe_smallest_ew, elem));
            } else {
                cache_for_queue = elem;
                break;
            }
        }
        if (yaku_entered) {
            yakushima::leave(yaku_token);
        }

        if (!release_rec_cont.empty()) {
            std::size_t erase_num{0};
            for (auto &&elem : release_rec_cont) {
                if (elem.first < maybe_smallest_ew) {
                    ++erase_num;
                    delete elem.second;// NOLINT
                } else {
                    break;
                }
            }
            if (erase_num != 0) {
                release_rec_cont.erase(release_rec_cont.begin(),
                                       release_rec_cont.begin() + erase_num);
            }
        }
    }

    /**
     * Free memory before shutdown.
     */
    for (auto &&elem : release_rec_cont) {
        delete elem.second; // NOLINT
    }
}

}//  namespace shirakami::cc_silo_variant::snapshot_manager