#pragma once

#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>

#include "shirakami/scheme.h"

namespace shirakami::bg_work {

class bg_commit {
public:
    /**
     * @brief First element of tuple is long tx id to sort container by priority
     * of long transactions.
     */
    using cont_type = std::set<std::tuple<std::size_t, Token>>;

    // start: getter
    static std::thread& worker_thread() { return worker_thread_; }

    [[nodiscard]] static bool worker_thread_end() { return worker_thread_end_; }

    static std::mutex& mtx_cont_wait_tx() { return mtx_cont_wait_tx_; }

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
    static inline std::thread worker_thread_; // NOLINT

    /**
     * @brief Flag used for signal to start or stop worker thread.
     * 
     */
    static inline bool worker_thread_end_; // NOLINT

    /**
     * @brief mutex for cont_wait_tx
     * 
     */
    static inline std::mutex mtx_cont_wait_tx_; // NOLINT

    /**
     * @brief container of long transactions waiting to commit.
     */
    static inline cont_type cont_wait_tx_; // NOLINT
};

} // namespace shirakami::bg_work