
#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/epoch_internal.h"

namespace shirakami::epoch {

void epoch_thread_work() {
    while (!get_epoch_thread_end()) {
        sleepMs(PARAM_EPOCH_TIME);
        set_global_epoch(get_global_epoch() + 1);
    }
}

void fin() {
    set_epoch_thread_end(true);
    join_epoch_thread();
}

void init() {
    set_epoch_thread_end(false);
    invoke_epoch_thread();
}

void invoke_epoch_thread() {
    // initialize
    set_epoch_thread_end(false);

    // invoking epoch thread
    epoch_thread = std::thread(epoch_thread_work);
}

} // namespace shirakami::epoch