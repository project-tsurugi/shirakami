/**
 * @file src/include/storage.h
 */

#pragma once

#include <atomic>
#include <shared_mutex>
#include <unordered_map>

#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"

namespace shirakami {

class storage {
public:
    static constexpr Storage initial_strg_ctr{1};
    /**
     * @brief Identifier for storing storage options.
     */
    static constexpr Storage meta_storage{UINT64_MAX - 1};
    /**
     * @brief Identifier for storing write preserve options.
     */
    static constexpr Storage wp_meta_storage{UINT64_MAX - 2};

    static Status key_handle_map_erase_storage(std::string_view const key) {
        std::lock_guard<std::shared_mutex> lk{mtx_key_handle_map_};
        auto ret = key_handle_map_.erase({std::string(key)});
        if (ret == 1) { return Status::OK; }
        return Status::WARN_NOT_FOUND;
    }

    static Status key_handle_map_get_storage(std::string_view const key,
                                             Storage& out) {
        std::shared_lock<std::shared_mutex> lk{mtx_key_handle_map_};
        auto itr = key_handle_map_.find(std::string(key));
        if (itr != key_handle_map_.end()) {
            // hit
            out = itr->second;
            return Status::OK;
        }
        return Status::WARN_NOT_FOUND;
    }

    static Status key_handle_map_push_storage(std::string_view const key,
                                              Storage const st) {
        std::lock_guard<std::shared_mutex> lk{mtx_key_handle_map_};
        auto ret = key_handle_map_.insert({std::string(key), st});
        if (ret.second) {
            // success
            return Status::OK;
        }
        return Status::WARN_ALREADY_EXISTS;
    }

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

    /**
     * @brief key handle map
     * @details key is storage's key given by outside. value is storage id 
     * given by internally.
     */
    static inline std::unordered_map<std::string, Storage> key_handle_map_;

    /**
     * @brief Mutex for key handle map.
     */
    static inline std::shared_mutex mtx_key_handle_map_;
};

} // namespace shirakami