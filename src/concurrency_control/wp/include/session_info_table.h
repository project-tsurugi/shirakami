/**
 * @file session_info_table.h
 * @brief core work about shirakami.
 */

#pragma once

#include <array>

#include "session_info.h"

namespace shirakami {

class session_info_table {
public:
    /**
     * @brief Check wheter the session is already started. This function is not
     * thread safe. But this function can be used only after taking mutex.
     */
    static Status decide_token(Token &token);  // NOLINT

    /**
     * @brief fin work about kThreadTable
     */
    static void fin_kThreadTable();

    static std::array<session_info, KVS_MAX_PARALLEL_THREADS> &
    get_thread_info_table() {  // NOLINT
        return kThreadTable;
    }

    /**
     * @brief init work about kThreadTable
     */
    static void init_kThreadTable();

private:
    /**
     * @details There are situations where you want to check table information and register / delete entries in the
     * table exclusively. When using exclusive lock, contention between readers is useless. When the reader writer lock
     * is used, the cache is frequently polluted by increasing or decreasing the reference count. Therefore, lock-free
     * exclusive arbitration is performed for fixed-length tables.
     * @attention This element may be used by CPR checkpointer, so please set KVS_MAX_PARALLEL_THREADS larger than
     * actual number of sessions.
     */
    static inline std::array<session_info, KVS_MAX_PARALLEL_THREADS>  // NOLINT
    kThreadTable;                                               // NOLINT
};

}  // namespace shirakami
