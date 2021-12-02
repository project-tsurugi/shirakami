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
    static constexpr Storage initial_strg_ctr{0};

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

    static std::vector<Storage>& get_reuse_num() { return reuse_num_; }

    static Status list_storage(std::vector<Storage>& out);

    static void set_strg_ctr(Storage st) {
        strg_ctr_.store(st, std::memory_order_release);
    }

private:
    static void get_new_storage_num(Storage& storage);

    static std::mutex& get_mt_reuse_num() { return mt_reuse_num_; }

    /**
     * @attention The number of storages above UINT64_MAX is undefined behavior.
     */
    static inline std::atomic<Storage> strg_ctr_{initial_strg_ctr};

    static inline std::vector<Storage> reuse_num_; // NOLINT

    static inline std::mutex mt_reuse_num_;
};

} // namespace shirakami