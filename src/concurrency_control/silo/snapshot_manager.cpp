
/**
 * @file snapshot_manager.cpp
 */

#include <glog/logging.h>

#include "include/snapshot_manager.h"

#ifdef CPR

#include "fault_tolerance/include/cpr.h"

#endif

#include "clock.h"
#include "compiler.h"
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
    std::pair<std::string, Record*> cache_for_queue{"", nullptr};

    while (likely(
            !snapshot_manager_thread_end.load(std::memory_order_acquire))) {
        // todo parametrize  for build options.
        sleepMs(1);

        epoch::epoch_t maybe_smallest_ew = epoch::get_global_epoch();
        if (maybe_smallest_ew != 0) --maybe_smallest_ew;

        yakushima::Token yaku_token{};
        bool yaku_entered{false};
        while (!remove_rec_cont.empty() || cache_for_queue.second != nullptr) {
            std::pair<std::string, Record*> elem{};
            if (cache_for_queue.second != nullptr) {
                elem = cache_for_queue;
                cache_for_queue = {"", nullptr};
            } else {
                if (!remove_rec_cont.try_pop(elem)) {
                    // Although there is an element in the contents, it failed to take out. Is there a false positive for the act of taking from the container?
                    break;
                }
            }

            if (elem.second->get_snap_ptr() == nullptr) {
                LOG(FATAL) << "fatal error";
            }
            if (snapshot_manager::get_snap_epoch(
                        elem.second->get_snap_ptr()->get_tidw().get_epoch()) !=
                snapshot_manager::get_snap_epoch(
                        maybe_smallest_ew)) { // todo : measures for
                // round-trip of epoch.
                if (!yaku_entered) {
                    yakushima::enter(yaku_token);
                    yaku_entered = true;
                }
                std::string key{};
                elem.second->get_tuple().get_key(key);
                yakushima::remove(yaku_token, elem.first, key);
                release_rec_cont.emplace_back(
                        std::make_pair(maybe_smallest_ew, elem.second));
            } else {
                cache_for_queue = elem;
                break;
            }
        }
        if (yaku_entered) { yakushima::leave(yaku_token); }

        if (!release_rec_cont.empty()) {
            std::size_t erase_num{0};
            for (auto&& elem : release_rec_cont) {
                if (elem.first < maybe_smallest_ew) {
#ifdef CPR
                    if (elem.second->get_version() + 1 >=
                        cpr::global_phase_version::get_gpv().get_version()) {
                        break;
                    }
#endif
                    ++erase_num;
                    delete elem.second; // NOLINT
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
    for (auto&& elem : release_rec_cont) {
        delete elem.second; // NOLINT
    }
}

} //  namespace shirakami::snapshot_manager