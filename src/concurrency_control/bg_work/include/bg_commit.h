#pragma once

#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>
#include <tuple>

#include "shirakami/scheme.h"

#include "shirakami/tx_state_notification.h"

namespace shirakami::bg_work {

class bg_commit {
public:
    /**
     * @brief First element of tuple is long tx id to sort container by priority
     * of long transactions.
     */
    static constexpr std::size_t th_num = 2;
    using cont_type = std::set<std::tuple<std::size_t, Token>>;
    using worker_cont_type = std::array<std::thread, th_num>;
    using used_ids_type = std::set<std::size_t>;

    // start: getter
    static worker_cont_type& workers() { return worker_threads_; }

    [[nodiscard]] static bool worker_thread_end() { return worker_thread_end_; }

    static used_ids_type& used_ids() { return used_ids_; }

    static std::mutex& mtx_used_ids() { return mtx_used_ids_; }

    static std::shared_mutex& mtx_cont_wait_tx() { return mtx_cont_wait_tx_; }

    static cont_type& cont_wait_tx() { return cont_wait_tx_; }
    // end: getter

    // start: setter
    static void worker_thread_end(bool tf) { worker_thread_end_ = tf; }
    // end: setter

    static void clear_tx();

    static void init();

    static void fin();

    static void register_tx(Token token);

    static void worker();

private:
    /**
     * @brief ltx commit verify threads
    */
    static inline worker_cont_type worker_threads_; // NOLINT

    /**
     * @brief This is a list what transaction do it be processed now by @a 
     * worker_threads_ to prevent conflict.
     * */
    static inline used_ids_type used_ids_; // NOLINT

    /**
     * @brief mutex for @a used_ids_
    */
    static inline std::mutex mtx_used_ids_; // NOLINT

    /**
     * @brief Flag used for signal to start or stop worker thread.
     * 
     */
    static inline bool worker_thread_end_; // NOLINT

    /**
     * @brief mutex for cont_wait_tx
     * 
     */
    static inline std::shared_mutex mtx_cont_wait_tx_; // NOLINT

    /**
     * @brief container of long transactions waiting to commit.
     */
    static inline cont_type cont_wait_tx_; // NOLINT
};

} // namespace shirakami::bg_work