/**
 * @file src/concurrency_control/wp/include/tuple_local.h
 */

#pragma once

#include <shared_mutex>
#include <string>
#include <string_view>

#include "shirakami/tuple.h"

namespace shirakami {

/**
 * @brief The class for return value of read operations.
 */
class Tuple::Impl {
public:
    Impl() = default;

    Impl(const Impl& right);

    Impl(std::string_view key, std::string_view val) : key_(key), val_(val) {}

    [[nodiscard]] std::string_view get_key() const { return key_; }

    [[nodiscard]] std::string get_val() {
        std::shared_lock<std::shared_mutex> lk(mtx_val_);
        return val_;
    }

    void set_key(std::string_view key) { key_ = key; }

    void set_val(std::string_view val) {
        std::lock_guard<std::shared_mutex> lk(mtx_val_);
        val_ = std::move(val);
    }

private:
    std::string_view key_{};
    std::string val_{};
    std::shared_mutex mtx_val_;
};

} // namespace shirakami