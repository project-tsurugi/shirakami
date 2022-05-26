/**
 * @file lpwal.h
 * @author takayuki tanabe
 * @brief pwal header for limestone
 * @version 0.1
 * @date 2022-05-26
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#pragma once

#include <string_view>

#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/scheme.h"

#include "fault_tolerance/include/limestone.h"
#include "limestone/api/log_channel.h"

namespace shirakami::lpwal {

class write_version_type {
public:
    using major_write_version_type = epoch::epoch_t;
    using minor_write_version_type = std::uint64_t;

    write_version_type(major_write_version_type ma, minor_write_version_type mi)
        : major_write_version_(ma), minor_write_version_(mi) {}

    static minor_write_version_type gen_minor_write_version(bool is_long,
                                                            std::uint64_t ts) {
        if (is_long) { return (ts << 63) | 1; } // NOLINT
        return (ts << 63);                      // NOLINT
    }

    [[nodiscard]] major_write_version_type get_major_write_version() const {
        return major_write_version_;
    }

    [[nodiscard]] minor_write_version_type get_minor_write_version() const {
        return minor_write_version_;
    }

    void set_major_write_version(major_write_version_type mv) {
        major_write_version_ = mv;
    }

    void set_minor_write_version_(std::uint64_t v) { minor_write_version_ = v; }

private:
    /**
     * @brief For PITR and major write version
     * 
     */
    major_write_version_type major_write_version_;

    /**
     * @brief The order in the same epoch.
     * @details bit layout:
     * 1 bits: 0 - short tx, 1 - long tx.
     * 63 bits: the order between short tx or long tx id.
     */
    minor_write_version_type minor_write_version_;
};

class log_record {
public:
    log_record(bool is_delete, write_version_type wv, Storage st,
               std::string_view key, std::string_view val)
        : is_delete_(is_delete), wv_(wv), st_(st), key_(key), val_(val) {}

    [[nodiscard]] bool get_is_delete() const { return is_delete_; }

    [[nodiscard]] std::string_view get_key() const { return key_; }

    [[nodiscard]] Storage get_st() const { return st_; }

    [[nodiscard]] std::string_view get_val() const { return val_; }

    [[nodiscard]] write_version_type get_wv() const { return wv_; }

    void set_is_delete(bool tf) { is_delete_ = tf; }

    void set_key(std::string_view v) { key_ = v; }

    void set_st(Storage st) { st_ = st; }

    void set_val(std::string_view v) { val_ = v; }

    void set_wv_(write_version_type wv) { wv_ = wv; }

private:
    /**
     * @brief About write operation. If this is true, this is a delete operation.
     * If this is false, this is write operation.
     */
    bool is_delete_{};

    /**
      * @brief timestamp
      */
    write_version_type wv_;

    /**
     * @brief storage id
     */
    Storage st_{};

    /**
     * @brief key info
     */
    std::string key_{};

    /**
     * @brief value info
     */
    std::string val_{};
};

/**
 * @brief This is a handle which each session has.
 */
class handler {
public:
    using logs_type = std::vector<log_record>;

    logs_type& get_logs() { return logs_; }

    epoch::epoch_t get_oldest_log_epoch() {
        if (logs_.empty()) { return 0; }
        return logs_.front().get_wv().get_major_write_version();
    }

    limestone::api::log_channel& get_log_channel() { return log_channel_; }

    void push_log(log_record const& log) { logs_.emplace_back(log); }

private:
    /**
     * @brief log records
     */
    logs_type logs_{};

    /**
     * @brief log channel
     */
    limestone::api::log_channel log_channel_{};
};

/**
 * @brief flushing log. 1: begin log_channel, 2: add_entry, 
 * 3: end log_channel
 */
void flush_log(handler& handle);

} // namespace shirakami::lpwal