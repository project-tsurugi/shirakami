
#include "clock.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::bg_work {

void bg_commit::clear_tx() {
    std::lock_guard<std::shared_mutex> lk_{mtx_cont_wait_tx()};
    cont_wait_tx().clear();
}

void bg_commit::init(std::size_t waiting_resolver_threads_num) {
    // send signal
    worker_thread_end(false);

    // set waiting resolver threads
    waiting_resolver_threads(waiting_resolver_threads_num);

    // set joined waiting resolver threads
    joined_waiting_resolver_threads(0);

    // invoke thread
    for (std::size_t i = 0; i < waiting_resolver_threads_num; ++i) {
        workers().emplace_back(worker);
    }
}

void bg_commit::fin() {
    // send signal
    worker_thread_end(true);

    // wait thread end
    for (auto&& elem : workers()) { elem.join(); }
    // cleanup workers
    workers().clear();

    /**
     * cleanup container because after next startup, manager thread will
     * misunderstand.
     */
    clear_tx();
}

void bg_commit::worker() {
    while (!worker_thread_end()) {
        sleepUs(epoch::get_global_epoch_time_us());
    }

    // normal termination, update joined_waiting_resolver
    std::size_t expected{joined_waiting_resolver_threads()};
    for (;;) {
        if (cas_joined_waiting_resolver_threads(expected, expected + 1)) {
            break;
        }
        // else: expected was update by actual value
    }
}

} // namespace shirakami::bg_work
