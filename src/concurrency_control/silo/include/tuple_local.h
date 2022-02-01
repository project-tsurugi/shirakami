/**
 * @file src/concurrency_control/silo/include/tuple_local.h
 * @brief header about Tuple::Impl
 */

#pragma once

#include <atomic>
#include <mutex>
#include <string>

#include "shirakami/tuple.h"

namespace shirakami {

class Tuple::Impl {
public:
    Impl() {} // NOLINT
    Impl(std::string_view key, std::string_view val);

    Impl(const Impl& right);

    Impl(Impl&& right);

    /**
     * @brief copy assign operator
     * @pre this is called by read_record function at xact.concurrency_control only .
     */
    Impl& operator=(const Impl& right); // NOLINT

    Impl& operator=(Impl&& right); // NOLINT

    /**
     * @brief Get the key object
     * @note key will not be changed.
     * @return std::string_view 
     */
    [[nodiscard]] std::string_view get_key() const; // NOLINT

    /**
     * @brief Get the value object
     * @note value may be changed.
     * @return std::string 
     */
    [[nodiscard]] std::string get_value(); // NOLINT

    void reset();

    void set(std::string_view const key, std::string_view const val) {
        key_ = key;
        std::unique_lock<std::mutex> lk{mtx_value_};
        value_ = val;
    }

    [[maybe_unused]] void set_key(std::string_view key) { key_ = key; }

    [[maybe_unused]] void set_value(std::string_view val) {
        std::unique_lock<std::mutex> lk{mtx_value_};
        value_ = val;
    }

private:
    std::string key_{};
    std::string value_{};
    std::mutex mtx_value_{};
};

} // namespace shirakami
