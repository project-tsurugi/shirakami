/**
 * @file bench/include/gen_tx.h
 * @brief generate operations of transaction.
 */

#pragma once

#include "./shirakami_string.h"

// shirakami/src/
#include "concurrency_control/silo_variant/include/scheme.h"

#include "logger.h"
#include "random.h"
#include "zipf.h"

using namespace shirakami::logger;

namespace shirakami {

// Operations for retry by abort
class opr_obj {  // NOLINT
public:
    opr_obj() = default;

    // for search
    opr_obj(const OP_TYPE type, std::string_view key)
            : type_(type), key_(key), val_() {}  // NOLINT

    // for scan / update
    opr_obj(const OP_TYPE type, std::string_view str1, std::string_view str2)
            : type_(type) {
        if (type == OP_TYPE::UPDATE) {
            key_ = str1;
            val_ = str2;
        } else if (type == OP_TYPE::SCAN) {
            scan_l_key_ = str1;
            scan_r_key_ = str2;
        }
    }

    opr_obj(const opr_obj &right) = delete;

    opr_obj(opr_obj &&right) = default;

    opr_obj &operator=(const opr_obj &right) = delete;  // NOLINT
    opr_obj &operator=(opr_obj &&right) = default;      // NOLINT

    ~opr_obj() = default;

    OP_TYPE get_type() { return type_; }  // NOLINT
    std::string_view get_key() {          // NOLINT
        return key_;
    }

    std::string_view get_scan_l_key() { // NOLINT
        return scan_l_key_;
    }

    std::string_view get_scan_r_key() { // NOLINT
        return scan_r_key_;
    }

    std::string_view get_value() {  // NOLINT
        return val_;
    }

    void set_type(OP_TYPE op) {
        type_ = op;
    }

    void set_scan_l_key(std::string_view key) {
        scan_l_key_ = key;
    }

    void set_scan_r_key(std::string_view key) {
        scan_r_key_ = key;
    }

private:
    OP_TYPE type_{};
    std::string key_{};
    std::string scan_l_key_{};
    std::string scan_r_key_{};
    std::string val_{};
};

/**
 * @brief generate search/update operations.
 */
static void
gen_tx_rw(std::vector<opr_obj> &opr_set, const std::size_t tpnm, const std::size_t opnm, const std::size_t rratio,
          const std::size_t val_length, Xoroshiro128Plus &rnd, FastZipf &zipf) {
    using namespace shirakami;
    opr_set.clear();
    for (std::size_t i = 0; i < opnm; ++i) {
        uint64_t keynm = zipf() % tpnm;
        uint64_t keybs = __builtin_bswap64(keynm);
        // std::unique_ptr<char[]> key = std::make_unique<char[]>(kKeyLength);
        // memcpy(key.get(), (std::to_string(keynm)).c_str(), kKeyLength);
        constexpr std::size_t thou = 100;
        if ((rnd.next() % thou) < rratio) {
            opr_set.emplace_back(OP_TYPE::SEARCH,
                                 std::string_view{reinterpret_cast<char*>(&keybs), sizeof(uint64_t)}); // NOLINT
        } else {
            std::string val(val_length, '0');  // NOLINT
            make_string(val, rnd);
            opr_set.emplace_back(OP_TYPE::UPDATE,
                                 std::string_view{reinterpret_cast<char*>(&keybs), sizeof(uint64_t)}, val); // NOLINT
        }
    }
}

static void
gen_tx_scan(std::vector<opr_obj> &opr_set, const std::size_t tpnm, const std::size_t scan_elem_n, Xoroshiro128Plus &rnd,
            FastZipf &zipf) {
    using namespace shirakami;
    opr_set.clear();
    uint64_t key_l_nm = zipf() % (tpnm - scan_elem_n + 1);
    uint64_t key_r_nm = key_l_nm + (scan_elem_n - 1);
    if (key_r_nm >= tpnm) {
        shirakami_logger->debug("fatal error.");
        exit(1);
    }
    uint64_t key_l_bs = __builtin_bswap64(key_l_nm);
    uint64_t key_r_bs = __builtin_bswap64(key_r_nm);
    opr_set.emplace_back(OP_TYPE::SCAN, std::string_view{reinterpret_cast<char*>(&key_l_bs), sizeof(uint64_t)}, // NOLINT
                         std::string_view{reinterpret_cast<char*>(&key_r_bs), sizeof(uint64_t)}); // NOLINT
}

}  // namespace shirakami
