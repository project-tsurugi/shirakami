
#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/wp/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::bg_work {

void bg_commit::init() {
    // send signal
    worker_thread_end() = false;

    // invoke thread
    worker_thread() = std::thread(worker());
}

void bg_commit::fin() {
    // send signal
    worker_thread_end() = true;

    // wait thread end
    worker_thread().join();
}

void bg_commit::register_tx(Token token) {
    auto* ti = static_cast<session*>(token);
    // check from long
    if (ti->get_tx_type() != transaction_options::transaction_type::LONG) {
        LOG(ERROR) << "unexpected error";
        return;
    }

    // lock for container
    std::lock_guard<std::shared_mutex> lk_{mtx_cont_wait_tx()};
    auto ret =
            cont_wait_tx().insert(std::make_tuple(ti->get_long_tx_id(), token));
    if (!ret.second) {
        // already exist
        LOG(ERROR) << "unexpected error";
    }
}

void bg_commit::worker() {
    while (!worker_thread_end()) {
        sleepMs(PARAM_EPOCH_TIME);
        // lock for container
        std::lock_guard<std::shared_mutex> lk_{mtx_cont_wait_tx()};
        for (auto itr = cont_wait_tx().begin(); itr != cont_wait_tx().end();
             ++itr) {
            Token token = std::get<1>(*itr);
            auto* ti = static_cast<session*>(token);
            // check from long
            if (ti->get_tx_type() !=
                        transaction_options::transaction_type::LONG ||
                ti->get_requested_commit(false)) {
                // not long or not requested commit.
                LOG(ERROR) << "unexpected error";
                return;
            }

            auto rc = shirakami::commit(token);
            if ()
        }
    }
}

} // namespace shirakami::bg_work