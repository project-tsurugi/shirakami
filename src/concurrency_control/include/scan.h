#pragma once

#include <map>
#include <shared_mutex>
#include <tuple>

#include <tbb/concurrent_hash_map.h>

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
        // for strand
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

class scan_cache {
public:
    using scan_elem_type =
            std::tuple<Storage,
                       std::vector<std::tuple<const Record*,
                                              yakushima::node_version64_body,
                                              yakushima::node_version64*>>>;

    using element_iterator_pair = std::pair<scan_elem_type, std::size_t>;

    using entity_type = tbb::concurrent_hash_map<ScanHandle, element_iterator_pair>;

    /**
     * @brief create empty object
     */
    scan_cache() = default;

    /**
     * @brief destruct object
     */
    ~scan_cache() = default;

    scan_cache(scan_cache const& other) = delete;
    scan_cache& operator=(scan_cache const& other) = delete;
    scan_cache(scan_cache&& other) noexcept = delete;
    scan_cache& operator=(scan_cache&& other) noexcept = delete;

    void clear() {
        entity_.clear();
    }

    bool erase(ScanHandle s) {
        return entity_.erase(s);
    }

    scan_elem_type* find(ScanHandle s) {
        decltype(entity_)::accessor acc{};
        if (! entity_.find(acc, s)) { //NOLINT
            return nullptr;
        }
        return std::addressof(acc->second.first);
    }

    bool create(ScanHandle s) {
        decltype(entity_)::accessor acc{};
        return entity_.insert(acc, s);
    }

    scan_elem_type& operator[](ScanHandle s) {
        decltype(entity_)::accessor acc{};
        entity_.insert(acc, s);
        return acc->second.first;
    }

private:
    entity_type entity_{};
};

class scan_cache_itr {
public:
    using entity_type = tbb::concurrent_hash_map<ScanHandle, std::size_t>;

    /**
     * @brief create empty object
     */
    scan_cache_itr() = default;

    /**
     * @brief destruct object
     */
    ~scan_cache_itr() = default;

    scan_cache_itr(scan_cache_itr const& other) = delete;
    scan_cache_itr& operator=(scan_cache_itr const& other) = delete;
    scan_cache_itr(scan_cache_itr&& other) noexcept = delete;
    scan_cache_itr& operator=(scan_cache_itr&& other) noexcept = delete;

    void clear() {
        entity_.clear();
    }

    bool erase(ScanHandle s) {
        return entity_.erase(s);
    }

    size_t* find(ScanHandle s) {
        decltype(entity_)::accessor acc{};
        if (! entity_.find(acc, s)) { //NOLINT
            return nullptr;
        }
        return std::addressof(acc->second);
    }

    bool create(ScanHandle s) {
        decltype(entity_)::accessor acc{};
        return entity_.insert(acc, s);
    }

    std::size_t& operator[](ScanHandle s) {
        decltype(entity_)::accessor acc{};
        entity_.insert(acc, s);
        return acc->second;
    }

private:
    entity_type entity_{};
};

class scan_handler {
public:
    using scan_elem_type =
            std::tuple<Storage,
                       std::vector<std::tuple<const Record*,
                                              yakushima::node_version64_body,
                                              yakushima::node_version64*>>>;
    using scan_cache_type = scan_cache;
    using scan_cache_itr_type = scan_cache_itr;
    static constexpr std::size_t scan_cache_storage_pos = 0;
    static constexpr std::size_t scan_cache_vec_pos = 1;

    void clear() {
        get_scan_cache().clear();
        get_scan_cache_itr().clear();
        get_scanned_storage_set().clear();
    }

    Status clear(ScanHandle hd) {
        // about scan cache
        if(! get_scan_cache().erase(hd)) {
            return Status::WARN_INVALID_HANDLE;
        }
        // about scan cache iterator
        get_scan_cache_itr().erase(hd);
        set_r_key(""); //FIXME
        set_r_end(scan_endpoint::EXCLUSIVE); //FIXME

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

    scanned_storage_set& get_scanned_storage_set() {
        return scanned_storage_set_;
    }

    [[nodiscard]] std::string_view get_r_key() const { return r_key_; }

    [[nodiscard]] scan_endpoint get_r_end() const { return r_end_; }

    // setter

    void set_r_key(std::string_view r_key) { r_key_ = r_key; }

    void set_r_end(scan_endpoint r_end) { r_end_ = r_end; }

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
     * @brief scanned storage set.
     * @details As a result of being scanned, the pointer to the record
     * is retained. However, it does not retain the scanned storage information
     * . Without it, you will have problems generating read sets.
     */
    scanned_storage_set scanned_storage_set_{};

    /**
     * @brief range of right endpoint for ltx
     * @details if user read to right endpoint till scan limit, shirakami needs
     * to know this information to log range info.
     */
    std::string r_key_{};

    scan_endpoint r_end_{};
};

} // namespace shirakami
