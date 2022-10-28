#pragma once

<<<<<<< HEAD
#include <map>
#include <set>
#include <shared_mutex>
#include <thread>
#include <tuple>
=======
#include <mutex>
>>>>>>> 1a8018aa68c4449bcd633374edb89b7acdfe5168

#include "shirakami/scheme.h"

namespace shirakami::bg_work {

class bg_commit {
public:
<<<<<<< HEAD
    /**
     * @brief First element of tuple is long tx id to sort container by priority
     * of long transactions.
     */
    using cont_type = std::set<std::tuple<std::size_t, Token>>;

    // start: getter
    std::thread& worker_thread() { return worker_thread_; }

    bool& worker_thread_end() { return worker_thread_end_; }

    std::shared_mutex& mtx_cont_wait_tx() { return mtx_cont_wait_tx_; }

    cont_type& cont_wait_tx() { return cont_wait_tx_; }
    // end: getter

    static void init();

    static void fin();

    static void register_tx();

    static void worker();

private:
    static inline std::thread worker_thread_;

    /**
     * @brief Flag used for signal to start or stop worker thread.
     * 
     */
    static inline bool worker_thread_end_;

    /**
     * @brief mutex for cont_wait_tx
     * 
     */
    static inline std::shared_mutex mtx_cont_wait_tx_;

    /**
     * @brief container of long transactions waiting to commit.
     */
    static inline container_type cont_wait_tx_;
=======
private:
    std::mutex mtx_cont_wait_tx;
    std::set<std::tuple<std::size_t, Token>> cont_wait_tx;
>>>>>>> 1a8018aa68c4449bcd633374edb89b7acdfe5168
};

} // namespace shirakami::bg_work