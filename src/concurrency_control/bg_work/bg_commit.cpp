
#include "clock.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/include/ongoing_tx.h"

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

    // invoke thread
    for (std::size_t i = 0; i < waiting_resolver_threads_num; ++i) {
        workers().emplace_back(std::thread(worker));
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

void bg_commit::register_tx(Token token) {
    auto* ti = static_cast<session*>(token);
    // check from long
    if (ti->get_tx_type() != transaction_options::transaction_type::LONG) {
        LOG(ERROR) << log_location_prefix << "unexpected error";
        return;
    }

    // lock for container
    {
        std::lock_guard<std::shared_mutex> lk_{mtx_cont_wait_tx()};
        auto ret = cont_wait_tx().insert(
                std::make_tuple(ti->get_long_tx_id(), token));
        if (!ret.second) {
            // already exist
            LOG(ERROR) << log_location_prefix << "unexpected error";
        }
    }
}

void bg_commit::worker() {
    while (!worker_thread_end()) {
        sleepUs(epoch::get_global_epoch_time_us());

        std::set<std::size_t> checked_ids = {};
        Token token{};
        std::size_t tx_id{};
        session* ti{};
    // find process tx
    REFIND: // NOLINT
    {
        std::shared_lock<std::shared_mutex> lk1{mtx_cont_wait_tx()};
        // if cont empty then clear used_ids
        if (cont_wait_tx().empty()) {
            {
                std::unique_lock<std::mutex> lk2{mtx_used_ids()};
                if (!used_ids().empty()) { used_ids().clear(); }
            }
            continue;
        }
        auto itr = cont_wait_tx().begin();
        for (; itr != cont_wait_tx().end(); ++itr) {
            token = std::get<1>(*itr);
            tx_id = std::get<0>(*itr);
            ti = static_cast<session*>(token);
            // check from long
            if (ti->get_tx_type() !=
                        transaction_options::transaction_type::LONG ||
                !ti->get_requested_commit()) {
                // not long or not requested commit.
                LOG(ERROR) << log_location_prefix << "unexpected error. "
                           << ti->get_tx_type() << ", "
                           << ti->get_requested_commit();
                return;
            }

            // check conflict between worker
            {
                std::unique_lock<std::mutex> lk2{mtx_used_ids()};
                // find by the id
                auto find_itr = used_ids().find(tx_id);
                if (find_itr != used_ids().end()) {
                    // found
                    continue;
                } // not found, not currently used
                // check already checked
                auto checked_itr = checked_ids.find(tx_id);
                if (checked_itr != checked_ids.end()) {
                    // found
                    continue;
                } // not found, not currently used and not checked
                used_ids().insert(tx_id);
                checked_ids.insert(tx_id);
                break;
            }
        }
        if (itr == cont_wait_tx().end()) {
            // reached last
            checked_ids.clear();
            continue;
        }
    }

        // process
        // try commit
        auto rc = shirakami::long_tx::commit(ti);
        // check result
        if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
            /**
              * Basically (without read area function), lower priority 
              * than this transaction wait for the result of this 
              * transaction.
              */
            {
                /**
                 * concurrent thread へコミット処理を許容する。checked_ids によって
                 * 自身は次の周回まで繰り返してトライすることは無い。
                */
                std::unique_lock<std::mutex> lk2{mtx_used_ids()};
                used_ids().erase(tx_id);
            }
            goto REFIND; // NOLINT
        }                // termination was successed
        ti->set_result_requested_commit(rc);

        // erase the tx from cont
        {
            std::lock_guard<std::shared_mutex> lk1{mtx_cont_wait_tx()};
            cont_wait_tx().erase(std::make_tuple(tx_id, token));
        }
        /**
         * used_ids から tx_id 要素を削除して並行スレッドへコミット処理を許容しては
         * ならない。なぜならコミット処理が同一TXに対して重複してエラーになる。待ち
         * リストが空になったら安全に used_ids をクリアする。
        */

        goto REFIND; // NOLINT
    }
}

void bg_commit::show_waiting() {
    std::vector<cont_type::key_type> cont_wait_tx_copy;
    {
        std::shared_lock<std::shared_mutex> lk1{mtx_cont_wait_tx()};
        VLOG(35) << "/:shirakami:diag317:bg_commit:show_waiting start";
        std::vector<cont_type::key_type> cont_wait_tx_copy2(cont_wait_tx().begin(), cont_wait_tx().end());
        cont_wait_tx_copy = std::move(cont_wait_tx_copy2);
    }
    for (auto e : cont_wait_tx_copy) {
        Token token = std::get<1>(e);
        std::size_t tx_id = std::get<0>(e);
        auto* ti = static_cast<session*>(token);
        // check from long
        if (ti->get_tx_type() != transaction_options::transaction_type::LONG ||
            !ti->get_requested_commit()) {
            // not long or not requested commit.
            LOG(ERROR) << log_location_prefix << "unexpected error";
            return;
        }
        Status rc{};
        bool need_wait = ongoing_tx::check_wait_for(ti, rc);
        VLOG(35) << "/:shirakami:diag317:bg_commit:show_waiting tx_id " << tx_id << " check done, need_wait = " << std::boolalpha << need_wait << ", rc = " << rc;
    }
    VLOG(35) << "/:shirakami:diag317:bg_commit:show_waiting end";
}

void bg_commit::show_list() {
    std::stringstream ss{};
    ss << "/:shirakami:wait_reason:bg_commit cont_wait:";
    {
        std::shared_lock<std::shared_mutex> lk1{mtx_cont_wait_tx()};
        auto itr = cont_wait_tx().begin();
        for (; itr != cont_wait_tx().end(); ++itr) {
            //auto token = std::get<1>(*itr);
            auto tx_id = std::get<0>(*itr);
            //auto ti = static_cast<session*>(token);
            ss << " " << tx_id;
        }
    }
    VLOG(10) << ss.str();
}

} // namespace shirakami::bg_work