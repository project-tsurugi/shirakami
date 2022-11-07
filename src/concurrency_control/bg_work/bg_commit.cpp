
#include "clock.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::bg_work {

void bg_commit::init() {
    // send signal
    worker_thread_end(false);

    // invoke thread
    worker_thread() = std::thread(worker);
}

void bg_commit::fin() {
    // send signal
    worker_thread_end(true);

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
    {
        std::lock_guard<std::shared_mutex> lk_{mtx_cont_wait_tx()};
        auto ret = cont_wait_tx().insert(
                std::make_tuple(ti->get_long_tx_id(), token));
        if (!ret.second) {
            // already exist
            LOG(ERROR) << "unexpected error";
        }
    }
}

void bg_commit::worker() {
    while (!worker_thread_end()) {
        sleepMs(PARAM_EPOCH_TIME);
        {
            // lock for container
            std::lock_guard<std::shared_mutex> lk_{mtx_cont_wait_tx()};
            for (auto itr = cont_wait_tx().begin();
                 itr != cont_wait_tx().end();) {
                Token token = std::get<1>(*itr);
                auto* ti = static_cast<session*>(token);
                // check from long
                if (ti->get_tx_type() !=
                            transaction_options::transaction_type::LONG ||
                    !ti->get_requested_commit()) {
                    // not long or not requested commit.
                    LOG(ERROR) << "unexpected error";
                    return;
                }

                // try commit
                auto rc = shirakami::long_tx::commit(ti);

                // check result
                if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                    /**
                      * Basically (without read area function), lower priority 
                      * than this transaction wait for the result of this 
                      * transaction.
                      */
                    /**
                      * choice 1: continue;
                      * choice 2: break;
                      */
                    ++itr;
                    continue;
                }
                ti->set_result_requested_commit(rc);
                itr = cont_wait_tx().erase(itr);
            }
        }
    }
}

} // namespace shirakami::bg_work