/**
 * @file src/include/storage.h
 */

#pragma once

#include <atomic>
#include <shared_mutex>
#include <unordered_map>

#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"

#include "glog/logging.h"

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

    /**
     * @brief Identifier for sequence storage.
     */
    static constexpr Storage sequence_storage{UINT64_MAX - 3};

    /**
     * @brief Identifier for internal development to express no read for read 
     * positive list
    */
    static constexpr Storage dummy_storage{UINT64_MAX - 4};

    static void fin();

    static void key_handle_map_clear() { key_handle_map_.clear(); }

    static void key_handle_map_display() {
        LOG(INFO) << ">> key_handle_map_display";
        for (auto&& itr : key_handle_map_) {
            LOG(INFO) << itr.first << ", " << itr.second;
        }
        LOG(INFO) << "<< key_handle_map_display";
    }
    static Status key_handle_map_erase_storage(std::string_view const key) {
        std::lock_guard<std::shared_mutex> lk{mtx_key_handle_map_};
        auto ret = key_handle_map_.erase({std::string(key)});
        if (ret == 1) { return Status::OK; }
        return Status::WARN_NOT_FOUND;
    }

    static Status key_handle_map_erase(Storage storage) {
        std::lock_guard<std::shared_mutex> lk{mtx_key_handle_map_};
        for (auto itr = key_handle_map_.begin(); // NOLINT
             itr != key_handle_map_.end(); ++itr) {
            if (itr->second == storage) {
                key_handle_map_.erase(itr);
                return Status::OK;
            }
        }
        return Status::WARN_NOT_FOUND;
    }

    /**
     * @brief 
     * @pre The caller of this func already got write lock about 
     * mtx_key_handle_map_.
     * @param[in] st The target handle
     * @param[out] out The binary string of the target handle
     */
    [[nodiscard]] static Status
    key_handle_map_erase_storage_without_lock(Storage st, std::string& out) {
        for (auto itr = key_handle_map_.begin(); // NOLINT
             itr != key_handle_map_.end(); ++itr) {
            if (itr->second == st) {
                out = itr->first;
                key_handle_map_.erase(itr);
                return Status::OK;
            }
        }
        return Status::WARN_NOT_FOUND;
    }

    static Status key_handle_map_get_storage(std::string_view const key,
                                             Storage& out) {
        std::shared_lock<std::shared_mutex> lk{mtx_key_handle_map_};
        return key_handle_map_get_storage_without_lock(key, out);
    }

    static Status
    key_handle_map_get_storage_without_lock(std::string_view const key,
                                            Storage& out) {
        auto itr = key_handle_map_.find(std::string(key));
        if (itr != key_handle_map_.end()) {
            // hit
            out = itr->second;
            return Status::OK;
        }
        return Status::WARN_NOT_FOUND;
    }

    static Status key_handle_map_get_key(Storage storage, std::string& out) {
        std::shared_lock<std::shared_mutex> lk{mtx_key_handle_map_};
        return key_handle_map_get_key_without_lock(storage, out);
    }

    static Status key_handle_map_get_key_without_lock(Storage storage,
                                                      std::string& out) {
        for (auto& itr : key_handle_map_) {
            if (itr.second == storage) {
                out = itr.first;
                return Status::OK;
            }
        }
        return Status::WARN_NOT_FOUND;
    }

    static Status key_handle_map_push_storage(std::string_view const key,
                                              Storage const st) {
        std::lock_guard<std::shared_mutex> lk{mtx_key_handle_map_};
        return key_handle_map_push_storage_without_lock(key, st);
    }

    static Status
    key_handle_map_push_storage_without_lock(std::string_view const key,
                                             Storage const st) {
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

    static void init_meta_storage();

    /**
     * @brief Create a storage object
     * @param[in] storage 
     * @param[in] options 
     * @return Status::OK success.
     */
    static Status register_storage(Storage storage,
                                   storage_option options = {}); // NOLINT

    /**
     * @brief Create a storage object
     * @param[in,out] storage The storage id specified by shirakami.
     * @param[in] options The storage options specified by caller.
     * @return Status::OK success.
     */
    static Status create_storage(Storage& storage,
                                 storage_option const& options);

    static Status create_storage(std::string_view key, Storage& storage,
                                 storage_option const& options);

    static Status exist_storage(Storage storage);

    /**
     * @brief delete storage
     * @param[in] storage
     */
    static Status delete_storage(Storage storage);

    static std::shared_mutex& get_mtx_key_handle_map() {
        return mtx_key_handle_map_;
    }

    static Storage get_strg_ctr() {
        return strg_ctr_.load(std::memory_order_acquire);
    }

    static Status list_storage(std::vector<Storage>& out);

    /**
     * @brief Get list of storage key
     * @param[out] out 
     * @return Status::OK success including out is empty.
     */
    static Status list_storage(std::vector<std::string>& out) {
        std::shared_lock<std::shared_mutex> lk{mtx_key_handle_map_};
        out.clear();
        out.reserve(key_handle_map_.size());
        for (auto& itr : key_handle_map_) { out.emplace_back(itr.first); }
        return Status::OK;
    }

    static void set_strg_ctr(Storage st) {
        strg_ctr_.store(st, std::memory_order_release);
    }

private:
    static void get_new_storage_num(Storage& storage);

    /**
     * @attention The number of storages above UINT64_MAX is undefined behavior.
     */
    static inline std::atomic<Storage> strg_ctr_{initial_strg_ctr}; // NOLINT

    /**
     * @brief key handle map
     * @details key is storage's key given by outside. value is storage id 
     * given by internally.
     */
    static inline std::unordered_map<std::string, Storage> // NOLINT
            key_handle_map_;                               // NOLINT

    /**
     * @brief Mutex for key handle map.
     */
    static inline std::shared_mutex mtx_key_handle_map_; // NOLINT
};

} // namespace shirakami