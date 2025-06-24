
#include <emmintrin.h>
#include <glog/logging.h>
#include <cstddef>

#include "database/include/thread_pool.h"
#include "concurrent_queue.h"

namespace shirakami {
class thread_task;

static void do_task([[maybe_unused]] thread_task* const out_task) {
    // todo task
    LOG(INFO);
}

void thread_pool::worker([[maybe_unused]] std::size_t const worker_id) {
    for (;;) {
        // try get task
        thread_task* out_task{};

        // if get task, do task
        if (get_task_queue().try_pop(out_task)) {
            do_task(out_task);
        } else {
            // else sleep
            _mm_pause();
        }

        // check flags and queue
        if (!thread_pool::get_running() && get_task_queue().empty()) { break; }
    }
}

} // namespace shirakami
