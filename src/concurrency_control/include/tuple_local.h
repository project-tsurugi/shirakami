/**
 * @file src/concurrency_control/include/tuple_local.h
 * @brief header about Tuple::Impl
 */

#pragma once

#include <atomic>
#include <shared_mutex>
#include <string>

#include "shirakami/tuple.h"

namespace shirakami {

class Tuple::Impl { // NOLINT
public:
    Impl() = default; // NOLINT

    Impl(std::string_view key, std::string_view val);

    ~Impl() = default; // NOLINT

    /**
     * @brief Get the key object
     * @param[out] out result
     * @note key will not be changed.
     * @return void
     */
    void get_key(std::string& out) const; // NOLINT

    /**
     * @brief Get the value object
     * @param[out] out result
     * @note value may be changed.
     * @return void
     */
    void get_value(std::string& out); // NOLINT

    void reset();

    void set(std::string_view const key, std::string_view const val) {
        key_ = key;
        value_ = val;
    }

    [[maybe_unused]] void set_key(std::string_view key) { key_ = key; }

    [[maybe_unused]] void set_value(std::string_view val) { value_ = val; }

private:
    std::string key_{};
    std::string value_{};
};

} // namespace shirakami
