/**
 * @file bench/include/gen_tx.h
 * @brief generate operations of transaction.
 */

#pragma once

#include "./shirakami_string.h"
#include "./ycsb_param.h"

// shirakami/src/
#include "cc/silo_variant/include/scheme.h"
#include "random.h"
#include "zipf.h"

namespace shirakami {

// Operations for retry by abort
class opr_obj {  // NOLINT
public:
    opr_obj() = default;

    opr_obj(const OP_TYPE type, std::string_view key)
            : type_(type), key_(key), val_() {}  // NOLINT
    opr_obj(const OP_TYPE type, std::string_view key, std::string_view val)
            : type_(type),
              key_(key),    // NOLINT
              val_(val) {}  // NOLINT

    opr_obj(const opr_obj &right) = delete;

    opr_obj(opr_obj &&right) = default;

    opr_obj &operator=(const opr_obj &right) = delete;  // NOLINT
    opr_obj &operator=(opr_obj &&right) = default;      // NOLINT

    ~opr_obj() = default;

    OP_TYPE get_type() { return type_; }  // NOLINT
    std::string_view get_key() {          // NOLINT
        return key_;
    }

    std::string_view get_value() {  // NOLINT
        return val_;
    }

private:
    OP_TYPE type_{};
    std::string key_{};
    std::string val_{};
};

/**
 * @brief generate search/update operations.
 */
static void gen_tx_rw(std::vector<opr_obj> &opr_set, const std::size_t tpnm,
                      const std::size_t opnm, const std::size_t rratio,
                      Xoroshiro128Plus &rnd, FastZipf &zipf) {
    using namespace shirakami;
    opr_set.clear();
    for (std::size_t i = 0; i < opnm; ++i) {
        uint64_t keynm = zipf() % tpnm;
        uint64_t keybs = __builtin_bswap64(keynm);
        // std::unique_ptr<char[]> key = std::make_unique<char[]>(kKeyLength);
        // memcpy(key.get(), (std::to_string(keynm)).c_str(), kKeyLength);
        constexpr std::size_t thou = 100;
        if ((rnd.next() % thou) < rratio) {
            opr_set.emplace_back(
                    OP_TYPE::SEARCH,
                    std::string_view{reinterpret_cast<char*>(&keybs),  // NOLINT
                                     sizeof(uint64_t)});
        } else {
            std::string val(ycsb_param::kValLength, '0');  // NOLINT
            make_string(val, rnd);
            opr_set.emplace_back(
                    OP_TYPE::UPDATE,
                    std::string_view{reinterpret_cast<char*>(&keybs),  // NOLINT
                                     sizeof(uint64_t)},
                    val);
        }
    }
}

}  // namespace shirakami
