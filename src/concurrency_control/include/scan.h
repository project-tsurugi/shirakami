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


/**
 * @brief scan cache entry
 * @details This class is to cache the result of the scan operation.
 * This class is NOT intended to be used by concurrent threads. Scan handle should not be shared between threads.
 */
class scan_cache {
public:
    using record_container_type =
            std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>>;

    using entity_type = std::tuple<Storage, record_container_type>;

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

    entity_type& entity() {
        return entity_;
    }

    std::size_t& scan_index() {
        return scan_index_;
    }

    void set_storage(Storage st) {
        std::get<0>(entity_) = st;
    }

    Storage get_storage() {
        return std::get<0>(entity_);
    }

    record_container_type& get_records() {
        return std::get<1>(entity_);
    }

    [[nodiscard]] std::string_view get_r_key() const {
        return r_key_;
    }

    void set_r_key(std::string_view r_key) {
        r_key_ = r_key;
    }

    [[nodiscard]] scan_endpoint get_r_end() const {
        return r_end_;
    }

    void set_r_end(scan_endpoint r_end) {
        r_end_ = r_end;
    }

private:
    entity_type entity_{};
    std::size_t scan_index_{0};

    /**
     * @brief range of right endpoint for ltx
     * @details if user read to right endpoint till scan limit, shirakami needs
     * to know this information to log range info.
     */
    std::string r_key_{};

    scan_endpoint r_end_{};
};

/**
 * @brief scan cache map
 * @details This class is to own and lookup scan cache entries.
 * This class is intended to be used by concurrent threads.
 */
class scan_cache_map {
public:

    using element_type = scan_cache;

    using entity_type = tbb::concurrent_hash_map<ScanHandle, element_type>;

    /**
     * @brief create empty object
     */
    scan_cache_map() = default;

    /**
     * @brief destruct object
     */
    ~scan_cache_map() = default;

    scan_cache_map(scan_cache_map const& other) = delete;
    scan_cache_map& operator=(scan_cache_map const& other) = delete;
    scan_cache_map(scan_cache_map&& other) noexcept = delete;
    scan_cache_map& operator=(scan_cache_map&& other) noexcept = delete;

    void clear() {
        entity_.clear();
    }

    bool erase(ScanHandle s) {
        return entity_.erase(s);
    }

    element_type* find(ScanHandle s) {
        decltype(entity_)::accessor acc{};
        if (! entity_.find(acc, s)) { //NOLINT
            return nullptr;
        }
        return std::addressof(acc->second);
    }

    bool register_handle(ScanHandle s) {
        decltype(entity_)::accessor acc{};
        return entity_.insert(acc, s);
    }

    /**
     * @brief special operator[]
     * @details lookup the scan cache or create new one, and return the tuple part.
     * @attention this function is kept for compatibility of old testcases. Use find() instead.
     */
    scan_cache::entity_type& operator[](ScanHandle s) {
        decltype(entity_)::accessor acc{};
        entity_.insert(acc, s);
        return acc->second.entity();
    }

private:
    entity_type entity_{};
};

class scan_handler {
public:
    static constexpr std::size_t scan_cache_storage_pos = 0;
    static constexpr std::size_t scan_cache_vec_pos = 1;

    void clear() {
        get_scan_cache().clear();
    }

    Status clear(ScanHandle hd) {
        // about scan cache
        if(! get_scan_cache().erase(hd)) {
            return Status::WARN_INVALID_HANDLE;
        }
        return Status::OK;
    }

    // getter

    [[maybe_unused]] scan_cache_map& get_scan_cache() { // NOLINT
        return map_;
    }

private:
    /**
     * @brief map handle to scan cache
     */
    scan_cache_map map_{};

};

} // namespace shirakami
