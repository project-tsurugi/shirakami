#pragma once

#include <string>

namespace shirakami {

[[maybe_unused]] static std::string make_key(std::size_t key_size, std::uint64_t base_num) {
    std::string gen_key{};
    if (key_size > sizeof(std::uint64_t)) {
        gen_key = std::string(key_size - sizeof(std::uint64_t), '0');
    }

    std::string key_buf{};
    if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__) {
        key_buf = std::string{reinterpret_cast<char*>(&base_num), sizeof(base_num)}; // NOLINT
    } else {
        std::uint64_t bs_buf = __builtin_bswap64(base_num);
        key_buf = std::string{reinterpret_cast<char*>(&bs_buf), sizeof(bs_buf)}; // NOLINT
    }

    if (key_size > sizeof(std::uint64_t)) {
        gen_key.append(key_buf);
        return gen_key;
    }
    return key_buf;
}

} // namespace shirakami
