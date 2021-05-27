/**
 * @file bench/include/gen_tx.h
 * @brief generate operations of transaction.
 */

#pragma once

#include <glog/logging.h>

#include "gen_key.h"
#include "random.h"
#include "zipf.h"

namespace shirakami {

// Operations for retry by abort
class opr_obj { // NOLINT
public:
    opr_obj() = default;

    // for search
    opr_obj(const OP_TYPE type, std::string_view key)
        : type_(type), key_(key) {} // NOLINT

    // for scan
    opr_obj(const OP_TYPE type, std::string_view str1, std::string_view str2)
        : type_(type) {
            scan_l_key_ = str1;
            scan_r_key_ = str2;
    }

    opr_obj(const opr_obj& right) = delete;

    opr_obj(opr_obj&& right) = default;

    opr_obj& operator=(const opr_obj& right) = delete; // NOLINT
    opr_obj& operator=(opr_obj&& right) = default;     // NOLINT

    ~opr_obj() = default;

    OP_TYPE get_type() { return type_; } // NOLINT
    std::string_view get_key() {         // NOLINT
        return key_;
    }

    std::string_view get_scan_l_key() { // NOLINT
        return scan_l_key_;
    }

    std::string_view get_scan_r_key() { // NOLINT
        return scan_r_key_;
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
};

/**
 * @brief generate search/update operations.
 */
static void
gen_tx_rw(std::vector<opr_obj>& opr_set, const std::size_t key_len, const std::size_t tpnm, const std::size_t opnm, const std::size_t rratio, Xoroshiro128Plus& rnd, FastZipf& zipf) {
    using namespace shirakami;
    opr_set.clear();
    for (std::size_t i = 0; i < opnm; ++i) {
        std::uint64_t keynm = zipf() % tpnm;
        constexpr std::size_t thou = 100;
        if ((rnd.next() % thou) < rratio) {
            opr_set.emplace_back(OP_TYPE::SEARCH, make_key(key_len, keynm)); // NOLINT
        } else {
            opr_set.emplace_back(OP_TYPE::UPDATE, make_key(key_len, keynm)); // NOLINT
        }
    }
}

static void
gen_tx_scan(std::vector<opr_obj>& opr_set, const std::size_t key_len, const std::size_t tpnm, const std::size_t scan_elem_n, Xoroshiro128Plus& rnd, FastZipf& zipf) {
    using namespace shirakami;
    opr_set.clear();
    uint64_t key_l_nm = zipf() % (tpnm - scan_elem_n + 1);
    uint64_t key_r_nm = key_l_nm + (scan_elem_n - 1);
    if (key_r_nm >= tpnm) {
        LOG(FATAL) << "fatal error";
    }
    opr_set.emplace_back(OP_TYPE::SCAN, make_key(key_len, key_l_nm), make_key(key_len, key_r_nm));
}

} // namespace shirakami
