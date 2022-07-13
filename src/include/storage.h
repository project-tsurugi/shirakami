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
    static constexpr Storage wp_meta_storage{UINT64_MAX - 1};

    /**
     * @brief initialization.
     * @pre This should be called before recovery.
     */
    static void init();

    /**
     * @brief Create a storage object
     * @param[in] storage 
     * @return Status::OK success.
     */
    static Status register_storage(Storage storage);

    /**
     * @brief Create a storage object
     * @param[in,out] storage The storage id specified by shirakami.
     * @param[in] storage_id The storage id specified by caller.
     * @return Status::OK success.
     */
    static Status create_storage(Storage& storage, Storage storage_id);

    static Status exist_storage(Storage storage);

    /**
     * @brief delete storage
     * @param[in] storage
     */
    static Status delete_storage(Storage storage);

    static Storage get_strg_ctr() {
        return strg_ctr_.load(std::memory_order_acquire);
    }

    static Status list_storage(std::vector<Storage>& out);

    static void set_strg_ctr(Storage st) {
        strg_ctr_.store(st, std::memory_order_release);
    }

private:
    static void get_new_storage_num(Storage& storage);

    /**
     * @attention The number of storages above UINT64_MAX is undefined behavior.
     */
    static inline std::atomic<Storage> strg_ctr_{initial_strg_ctr};
};

} // namespace shirakami