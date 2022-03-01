/**
 * @file src/include/storage.h
 */

#pragma once

#include <atomic>
#include <mutex>

#include "shirakami/scheme.h"

namespace shirakami {

class storage {
public:
    /**
     * @brief left endpoint of storage range. [1M: ?).
     * [0:1M) is for other system.
     * 
     */
    static constexpr Storage initial_strg_ctr{1000 * 1000};

    /**
     * @brief initialization.
     * @pre This should be called before recovery.
     */
    static void init();

    static Status register_storage(Storage& storage);

    static Status exist_storage(Storage storage);

    /**
     * @brief delete storage
     * @param[in] storage
     */
    static Status delete_storage(Storage storage);

    static Storage get_strg_ctr() {
        return strg_ctr_.load(std::memory_order_acquire);
    }

    static std::mutex& get_mt_reuse_num() { return mt_reuse_num_; }

    static std::vector<Storage>& get_reuse_num() { return reuse_num_; }

    static Status list_storage(std::vector<Storage>& out);

    static void register_reuse_num(Storage st) {
        std::unique_lock lk {mt_reuse_num_};
        reuse_num_.emplace_back(st);
    }

    static void set_strg_ctr(Storage st) {
        strg_ctr_.store(st, std::memory_order_release);
    }

private:
    static void get_new_storage_num(Storage& storage);

    /**
     * @attention The number of storages above UINT64_MAX is undefined behavior.
     */
    static inline std::atomic<Storage> strg_ctr_{initial_strg_ctr};

    static inline std::vector<Storage> reuse_num_; // NOLINT

    static inline std::mutex mt_reuse_num_;
};

} // namespace shirakami