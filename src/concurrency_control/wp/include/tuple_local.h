/**
 * @file src/concurrency_control/wp/include/tuple_local.h
 */

#pragma once

#include <string>
#include <string_view>

#include "shirakami/tuple.h"

namespace shirakami {

/**
 * @brief The class for return value of read operations.
 */
class Tuple::Impl {
public:
    Impl() {} // NOLINT

    [[nodiscard]] std::string_view get_key() const { return key_; }

    [[nodiscard]] std::string_view get_val() const { return val_; }

    void set_key(std::string_view key) { key_ = key; }

    void set_val(std::string_view val) { val_ = val; }

private:
    std::string key_{};
    std::string val_{};
};

} // namespace shirakami