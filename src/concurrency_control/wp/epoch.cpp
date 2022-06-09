
#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/epoch_internal.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

#ifdef PWAL

#include "datastore/limestone/include/datastore.h"

#endif

#include "glog/logging.h"

namespace shirakami::epoch {

inline void check_epoch_load_and_update_idle_living_tx() {
    auto ce{epoch::get_global_epoch()};
    for (auto&& itr : session_table::get_session_table()) {
        if (!itr.get_operating()) {
            // this session is not processing now.
            if (itr.get_step_epoch() < ce) { itr.set_step_epoch(ce); }
        }
    }
}

void epoch_thread_work() {
    while (!get_epoch_thread_end()) {
        sleepMs(PARAM_EPOCH_TIME);
        {
            auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());
            std::unique_lock<std::mutex> lk{get_ep_mtx()};
            for (;;) {
                auto ptp{epoch::get_perm_to_proc()};
                if (ptp < -1) {
                    LOG(ERROR) << "programming error";
                    return;
                }
                if (ptp == -1) {
                    // ptp invalid
                    break;
                }
                if (ptp == 0) {
                    // wait to lock release
                    _mm_pause();
                } else {
                    // ptp allow epoch inclement
                    epoch::set_perm_to_proc(ptp - 1);
                    break;
                }
            }
            // change epoch
            auto new_epoch{get_global_epoch() + 1};
            set_global_epoch(new_epoch);
#ifdef PWAL
            // before changing epoch, flush log of this epoch

            // change also datastore's epoch
            shirakami::datastore::get_datastore()->switch_epoch(new_epoch);
#endif
            // dtor : release wp_mutex
        }
        check_epoch_load_and_update_idle_living_tx();
    }
}

void fin() {
    set_epoch_thread_end(true);
    join_epoch_thread();
}

void init() { invoke_epoch_thread(); }

void invoke_epoch_thread() {
    // initialize
    set_epoch_thread_end(false);

    // invoking epoch thread
    epoch_thread = std::thread(epoch_thread_work);
}

} // namespace shirakami::epoch