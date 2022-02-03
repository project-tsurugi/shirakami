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

class Tuple::Impl {
public:
    Impl() = default;

    Impl(std::string_view key, std::string_view val);

    Impl(const Impl& right);

    Impl(Impl&& right);

    ~Impl() = default;

    /**
     * @brief copy assign operator
     * @pre this is called by read_record function at xact.concurrency_control only .
     */
    Impl& operator=(const Impl& right); // NOLINT

    Impl& operator=(Impl&& right); // NOLINT

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
        std::lock_guard<std::shared_mutex> lk{mtx_value_};
        value_ = val;
    }

    [[maybe_unused]] void set_key(std::string_view key) { key_ = key; }

    [[maybe_unused]] void set_value(std::string_view val) {
        std::lock_guard<std::shared_mutex> lk{mtx_value_};
        value_ = val;
    }

private:
    std::string key_{};
    std::string value_{};
    std::shared_mutex mtx_value_{};
};

} // namespace shirakami
