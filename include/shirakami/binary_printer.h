/*
 * Copyright 2018-2020 tsurugi project.
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

#include <iomanip>

namespace shirakami {

/**
 * @brief debug support to print binary value array
 */
class binary_printer {
public:
    constexpr binary_printer(void const* ptr, std::size_t size) noexcept
        : ptr_(ptr), size_(size) {}

    constexpr explicit binary_printer(std::string_view s) noexcept
        : ptr_(s.data()), size_(s.size()) {}

    friend std::ostream& operator<<(std::ostream& out,
                                    binary_printer const& value) {
        std::ios init(nullptr);
        init.copyfmt(out);
        for (std::size_t idx = 0; idx < value.size_; ++idx) {
            auto c = *(static_cast<std::uint8_t const*>(value.ptr_) + // NOLINT
                       idx);
            if (std::isprint(c) != 0) {
                out << c;
            } else {
                out << std::string_view("\\x");
                out << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<std::uint32_t>(
                               *(static_cast<std::uint8_t const*>(
                                         value.ptr_) + // NOLINT
                                 idx));
            }
        }
        out.copyfmt(init);
        return out;
    }

private:
    void const* ptr_{};
    std::size_t size_{};
};

} // namespace shirakami