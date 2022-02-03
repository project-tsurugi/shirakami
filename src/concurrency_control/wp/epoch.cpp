
#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/epoch_internal.h"
#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/include/tuple_local.h"

namespace shirakami::epoch {

void epoch_thread_work() {
    while (!get_epoch_thread_end()) {
        sleepMs(PARAM_EPOCH_TIME);
        auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());
        std::unique_lock<std::mutex> lk{get_ep_mtx()};
        set_global_epoch(get_global_epoch() + 1);
        // dtor : release wp_mutex
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