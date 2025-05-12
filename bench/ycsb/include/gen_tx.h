/**
 * @file bench/include/gen_tx.h
 * @brief generate operations of transaction.
 */

#pragma once

#include <cstdint>

#include "gen_key.h"
#include "random.h"
#include "zipf.h"

#include "shirakami/logging.h"
#include "shirakami/scheme.h"

#include "glog/logging.h"

namespace shirakami {

// Operations for retry by abort
class opr_obj { // NOLINT
public:
    opr_obj() = default;

    // for search
    opr_obj(OP_TYPE type, std::string_view key)
        : type_(type), key_(key) {} // NOLINT

    // for scan
    opr_obj(OP_TYPE type, std::string_view str1, std::string_view str2)
        : type_(type), scan_l_key_(str1), scan_r_key_(str2) {}

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

    void set_type(OP_TYPE op) { type_ = op; }

    void set_scan_l_key(std::string_view key) { scan_l_key_ = key; }

    void set_scan_r_key(std::string_view key) { scan_r_key_ = key; }

private:
    OP_TYPE type_{};
    std::string key_{};
    std::string scan_l_key_{};
    std::string scan_r_key_{};
};

/**
 * @brief generate search/update operations.
 * @param[in,out] opr_set
 * @param[in] key_len
 * @param[in] tpnm number of tuple.
 * @param[in] thread_num number of worker thread.
 * @param[in] thid id of thread. start from zero.
 * @param[in] opnm
 * @param[in] ops_read_type point or range.
 * @param[in] ops_write_type update or insert or readmodifywrite.
 * @param[in] rratio
 * @param[in,out] rnd
 * @param[in,out] zipf
 */
static void gen_tx_rw(std::vector<opr_obj>& opr_set, const std::size_t key_len,
                      const std::size_t tpnm, const std::size_t thread_num,
                      const std::size_t thid, const std::size_t opnm,
                      const std::string& ops_read_type,
                      const std::string& ops_write_type,
                      const std::size_t rratio, Xoroshiro128Plus& rnd,
                      FastZipf& zipf) {
    using namespace shirakami;
    opr_set.clear();
    for (std::size_t i = 0; i < opnm; ++i) {
        std::uint64_t keynm = zipf() % tpnm;
        if ((rnd.next() % 100) < rratio) { // NOLINT
            if (ops_read_type == "point") {
                opr_set.emplace_back(OP_TYPE::SEARCH,
                                     make_key(key_len, keynm)); // NOLINT
            } else if (ops_read_type == "range") {
                std::string key1 = make_key(key_len, keynm);
                keynm = zipf() % tpnm;
                std::string key2 = make_key(key_len, keynm);
                bool key1_is_small = key1 < key2;
                opr_set.emplace_back(OP_TYPE::SCAN, key1_is_small ? key1 : key2,
                                     key1_is_small ? key2 : key1); // NOLINT
            } else {
                LOG(FATAL) << "invalid read type";
            }
        } else {
            if (ops_write_type == "update") {
                opr_set.emplace_back(OP_TYPE::UPDATE,
                                     make_key(key_len, keynm)); // NOLINT
            } else if (ops_write_type == "insert") {
                // re-comp keynm
                // range: tpnm, each partition block per thread...
                std::size_t block_size = (SIZE_MAX - tpnm) / thread_num;
                std::size_t offset = rnd.next() % block_size;
                keynm = tpnm + (block_size * thid) + offset;
                opr_set.emplace_back(OP_TYPE::INSERT,
                                     make_key(key_len, keynm)); // NOLINT
            } else if (ops_write_type == "readmodifywrite") {
                opr_set.emplace_back(OP_TYPE::SEARCH,
                                     make_key(key_len, keynm)); // NOLINT
                opr_set.emplace_back(OP_TYPE::UPDATE,
                                     make_key(key_len, keynm)); // NOLINT
            }
        }
    }
}

/**
 * @param[in,out] opr_set
 * @param[in] key_len
 * @param[in] tpnm
 * @param[in] scan_elem_n
 * @param[in,out] rnd
 * @param[in,out] zipf
 */
static void gen_tx_scan(std::vector<opr_obj>& opr_set,
                        const std::size_t key_len, const std::size_t tpnm,
                        const std::size_t scan_elem_n, Xoroshiro128Plus& rnd,
                        FastZipf& zipf) {
    using namespace shirakami;
    opr_set.clear();
    uint64_t key_l_nm = zipf() % (tpnm - scan_elem_n + 1);
    uint64_t key_r_nm = key_l_nm + (scan_elem_n - 1);
    if (key_r_nm >= tpnm) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "fatal error";
    }
    opr_set.emplace_back(OP_TYPE::SCAN, make_key(key_len, key_l_nm),
                         make_key(key_len, key_r_nm));
}

} // namespace shirakami
