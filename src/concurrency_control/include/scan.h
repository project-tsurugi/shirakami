#pragma once

#include <map>
#include <shared_mutex>
#include <tuple>

#include "shirakami/logging.h"
#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

class scanned_storage_set {
public:
    Storage get(ScanHandle const hd) {
        std::shared_lock<std::shared_mutex> lk{get_mtx()};
        return map_[hd];
    }

    void clear() {
        std::lock_guard<std::shared_mutex> lk{get_mtx()};
        map_.clear();
    }

    void clear(ScanHandle const hd) {
        std::lock_guard<std::shared_mutex> lk{get_mtx()};
        map_.erase(hd);
    }

    void set(ScanHandle const hd, Storage const st) {
        std::lock_guard<std::shared_mutex> lk{get_mtx()};
        map_[hd] = st;
    };

    std::shared_mutex& get_mtx() { return mtx_; }

private:
    std::map<ScanHandle, Storage> map_;

    /**
     * @brief mutex for scanned storage set
    */
    std::shared_mutex mtx_{};
};

class scan_handler {
public:
    using scan_elem_type =
            std::tuple<Storage,
                       std::vector<std::tuple<const Record*,
                                              yakushima::node_version64_body,
                                              yakushima::node_version64*>>>;
    using scan_cache_type = std::map<ScanHandle, scan_elem_type>;
    using scan_cache_itr_type = std::map<ScanHandle, std::size_t>;
    static constexpr std::size_t scan_cache_storage_pos = 0;
    static constexpr std::size_t scan_cache_vec_pos = 1;

    void clear() {
        {
            std::lock_guard<std::shared_mutex> lk{get_mtx_scan_cache()};
            get_scan_cache().clear();
            get_scan_cache_itr().clear();
        }
        get_scanned_storage_set().clear();
    }

    Status clear(ScanHandle hd) {
        // about scan cache
        {
            std::lock_guard<std::shared_mutex> lk{get_mtx_scan_cache()};
            auto itr = get_scan_cache().find(hd);
            if (itr == get_scan_cache().end()) {
                return Status::WARN_INVALID_HANDLE;
            }
            get_scan_cache().erase(itr);

            // about scan cache iterator
            auto index_itr = get_scan_cache_itr().find(hd);
            get_scan_cache_itr().erase(index_itr);
            set_is_full_scan(false);
        }

        // about scanned storage set
        scanned_storage_set_.clear(hd);

        return Status::OK;
    }

    // getter

    [[maybe_unused]] scan_cache_type& get_scan_cache() { // NOLINT
        return scan_cache_;
    }

    [[maybe_unused]] scan_cache_itr_type& get_scan_cache_itr() { // NOLINT
        return scan_cache_itr_;
    }

    std::shared_mutex& get_mtx_scan_cache() { return mtx_scan_cache_; }

    scanned_storage_set& get_scanned_storage_set() {
        return scanned_storage_set_;
    }

    [[nodiscard]] bool get_is_full_scan() const { return is_full_scan_; }

    [[nodiscard]] std::string_view get_r_key() const { return r_key_; }

    // setter

    void set_is_full_scan(bool tf) { is_full_scan_ = tf; }

    void set_r_key(std::string_view r_key) { r_key_ = r_key; }

private:
    /**
     * @brief cache of index scan.
     */
    scan_cache_type scan_cache_{};

    /**
     * @brief cursor of the scan_cache_.
     */
    scan_cache_itr_type scan_cache_itr_{};

    /**
     * @brief mutex for scan cache
    */
    std::shared_mutex mtx_scan_cache_{};

    /**
     * @brief scanned storage set.
     * @details As a result of being scanned, the pointer to the record 
     * is retained. However, it does not retain the scanned storage information
     * . Without it, you will have problems generating read sets.
     */
    scanned_storage_set scanned_storage_set_{};

    bool is_full_scan_{false};

    /**
     * @brief range of right endpoint for ltx
     * @details if user read to right endpoint till scan limit, shirakami needs
     * to know this information to log range info.
    */
    std::string r_key_{};
};

} // namespace shirakami