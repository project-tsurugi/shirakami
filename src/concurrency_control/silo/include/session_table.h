/**
 * @file concurrency_control/silo/include/session_table.h
 * @brief core work about shirakami.
 */

#pragma once

#include <array>

#include "session.h"

namespace shirakami {

class session_table {
public:
    /**
     * @brief Acquire right of an one session.
     */
    static Status decide_token(Token& token); // NOLINT

    /**
     * @brief End work about session_table.
     */
    static void fin_session_table();

    /**
     * @brief getter of session_table_
     */
    static std::array<session, KVS_MAX_PARALLEL_THREADS>&
    get_session_table() { // NOLINT
        return session_table_;
    }

    /**
     * @brief Initialization about session_table_
     * @pre If it was a recovery boot, this function can be called after the recovery is complete.
     * @param[in] enable_recovery When this variable is true, all recovered records are logged as log records.
     */
    static void init_session_table(bool enable_recovery);

#ifdef CPR
    static void display_diff_set();

    static bool is_empty_logs();

#endif

private:
    /**
     * @brief The table holding session information.
     * @details There are situations where you want to check table information and register / delete entries in the
     * table exclusively. When using exclusive lock, contention between readers is useless. When the reader writer lock
     * is used, the cache is frequently polluted by increasing or decreasing the reference count. Therefore, lock-free
     * exclusive arbitration is performed for fixed-length tables.
     * @attention Please set KVS_MAX_PARALLEL_THREADS larger than actual number of sessions.
     */
    static inline std::array<session, KVS_MAX_PARALLEL_THREADS> // NOLINT
            session_table_;                                     // NOLINT
};

} // namespace shirakami
