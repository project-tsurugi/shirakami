/*
 * Copyright 2025-2025 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <ostream>
#include <string_view>
#include <vector>

namespace shirakami {

/**
 * @brief debug support to print C++20-std::span-like range
 */
template<typename T>
class span_printer {
public:
    constexpr explicit span_printer(const std::vector<T>& vec) noexcept
        : data_(vec.data()), size_(vec.size()), sep_(", ") {}
    constexpr span_printer(const std::vector<T>& vec, std::string_view sep) noexcept
        : data_(vec.data()), size_(vec.size()), sep_(sep) {}
    constexpr span_printer(const T* ptr, std::size_t size) noexcept
        : data_(ptr), size_(size), sep_(", ") {}
    constexpr span_printer(const T* ptr, std::size_t size, std::string_view sep) noexcept
        : data_(ptr), size_(size), sep_(sep) {}

    friend std::ostream& operator<<(std::ostream& out,
                                    span_printer const& value) {
        out << '[';
        for (std::size_t i = 0; i < value.size_; i++) {
            if (i > 0) {
                out << value.sep_;
            }
            out << value.data_[i]; // NOLINT
        }
        out << ']';
        return out;
    }

private:
    const T* data_{};
    std::size_t size_{};
    std::string_view sep_{};
};

} // namespace shirakami
