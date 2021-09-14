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
    static Status register_storage(Storage& storage);

    static Status exist_storage(Storage storage);

    /**
     * @brief delete storage
     * @param[in] storage
     */
    static Status delete_storage(Storage storage);

    static Status list_storage(std::vector<Storage>& out);

private:

    static void get_new_storage_num(Storage& storage);

    static std::mutex& get_mt_reuse_num() { return mt_reuse_num_; }

    static std::vector<Storage>& get_reuse_num() { return reuse_num_; }

    /**
     * @attention The number of storages above UINT64_MAX is undefined behavior.
     */
    static inline std::atomic<Storage> strg_ctr_{1};

    static inline std::vector<Storage> reuse_num_; // NOLINT

    static inline std::mutex mt_reuse_num_;
};

} // namespace shirakami