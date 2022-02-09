#pragma once

#include <map>
#include <tuple>

#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

class cursor_info {
private:
    enum class op_type {
        read,
        write,
    };

public:
    bool check_was_read(cursor_info::op_type op) {
        if (op == op_type::read) {
            return was_read_.test(0);
        } else if (op == op_type::write) {
            return was_read_.test(1);
        } else {
            LOG(FATAL);
        }
    }
    void get_key(std::string& out) { out = key_; }

    void get_value(std::string& out) { out = value_; }

    void set_key(std::string_view key) { key_ = key; }

    void set_value(std::string_view value) { value_ = value; }

    void set_was_read(cursor_info::op_type op) {
        if (op == op_type::read) {
            was_read_.set(0);
        } else if (op == op_type::write) {
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

class scan_handler {
public:
    using scan_cache_type = std::map<
            ScanHandle,
            std::tuple<Storage,
                       std::vector<std::tuple<const Record*,
                                              yakushima::node_version64_body,
                                              yakushima::node_version64*>>>>;
    using scan_cache_itr_type = std::map<ScanHandle, std::size_t>;
    static constexpr std::size_t scan_cache_storage_pos = 0;
    static constexpr std::size_t scan_cache_vec_pos = 1;

    [[maybe_unused]] scan_cache_type& get_scan_cache() { // NOLINT
        return scan_cache_;
    }

    [[maybe_unused]] scan_cache_itr_type& get_scan_cache_itr() { // NOLINT
        return scan_cache_itr_;
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
     * @brief 
     * @attention
     */
    cursor_info ci_{};
};

} // namespace shirakami