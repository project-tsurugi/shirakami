#pragma once

#include <map>
#include <tuple>

#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

class cursor_info {
public:
    enum class op_type {
        key,
        value,
    };

    bool check_was_read(cursor_info::op_type op) {
        if (op == op_type::key) { return was_read_.test(0); }
        if (op == op_type::value) { return was_read_.test(1); }
        LOG(FATAL);
    }

    void get_key(std::string& out) { out = key_; }

    void get_value(std::string& out) { out = value_; }

    bool get_was_read(cursor_info::op_type op) {
        if (op == op_type::key) { return was_read_.test(0); }
        if (op == op_type::value) { return was_read_.test(1); }
        LOG(FATAL);
        return true;
    }

    void set_key(std::string_view key) { key_ = key; }

    void set_value(std::string_view value) { value_ = value; }

    void set_was_read(cursor_info::op_type op) {
        if (op == op_type::key) {
            was_read_.set(0);
        } else if (op == op_type::value) {
            was_read_.set(1);
        } else {
            LOG(FATAL);
        }
    }

    void reset() {
        reset_was_read();
        key_.clear();
        value_.clear();
    }

    void reset_was_read() { was_read_.reset(); }

private:
    /**
     * @brief position 0 means whether it already read key. position 1 means 
     * whether it already read value.
     */
    std::bitset<2> was_read_;
    std::string key_{};
    std::string value_{};
};

class cursor_set {
public:
    cursor_info& get(ScanHandle hd) { return cset_[hd]; }

    void clear() { cset_.clear(); }

    void clear(ScanHandle hd) { cset_.erase(hd); }

private:
    std::map<ScanHandle, cursor_info> cset_;
};

class scanned_storage_set {
public:
    Storage get(ScanHandle const hd) { return map_[hd]; }

    void clear() { map_.clear(); }

    void clear(ScanHandle const hd) { map_.erase(hd); }

    void set(ScanHandle const hd, Storage const st) { map_[hd] = st; };

private:
    std::map<ScanHandle, Storage> map_;
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
        get_scan_cache().clear();
        get_scan_cache_itr().clear();
        cs_.clear();
    }

    Status clear(ScanHandle hd) {
        // about scan cache
        auto itr = get_scan_cache().find(hd);
        if (itr == get_scan_cache().end()) {
            return Status::WARN_INVALID_HANDLE;
        }
        get_scan_cache().erase(itr);

        // about scan cache iterator
        auto index_itr = get_scan_cache_itr().find(hd);
        get_scan_cache_itr().erase(index_itr);
        cs_.clear(hd);

        // about scanned storage set
        scanned_storage_set_.clear(hd);

        return Status::OK;
    }

    cursor_info& get_ci(ScanHandle hd) { return cs_.get(hd); }

    [[maybe_unused]] scan_cache_type& get_scan_cache() { // NOLINT
        return scan_cache_;
    }

    [[maybe_unused]] scan_cache_itr_type& get_scan_cache_itr() { // NOLINT
        return scan_cache_itr_;
    }

    scanned_storage_set& get_scanned_storage_set() {
        return scanned_storage_set_;
    }

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
     * @brief 
     * @attention
     */
    cursor_set cs_{};
};

} // namespace shirakami