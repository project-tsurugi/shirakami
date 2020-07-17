/**
 * @file
 * @brief generate operations of transaction.
 */

#pragma once

#include "string.hh"
#include "ycsb_param.h"

// shirakami/src/
#include "random.hh"
#include "scheme.hh"
#include "zipf.hh"

/**
 * @brief generate search/update operations.
 */
static void gen_tx_rw(std::vector<kvs::OprObj>& opr_set, std::size_t tpnm,
                      std::size_t opnm, std::size_t rratio,
                      Xoroshiro128Plus& rnd, FastZipf& zipf) {
  using namespace kvs;
  for (auto i = 0; i < opnm; ++i) {
    uint64_t keynm = zipf() % tpnm;
    uint64_t keybs = __builtin_bswap64(keynm);
    // std::unique_ptr<char[]> key = std::make_unique<char[]>(kKeyLength);
    // memcpy(key.get(), (std::to_string(keynm)).c_str(), kKeyLength);
    constexpr std::size_t thou = 100;
    if ((rnd.next() % thou) < rratio) {
      opr_set.emplace_back(OP_TYPE::SEARCH,
                           reinterpret_cast<char*>(&keybs),  // NOLINT
                           sizeof(uint64_t));
    } else {
      std::string value(ycsb_param::kValLength, '0'); // NOLINT
      make_string(value, rnd);
      opr_set.emplace_back(OP_TYPE::UPDATE,
                           reinterpret_cast<char*>(&keybs),  // NOLINT
                           sizeof(uint64_t), value.data(),
                           ycsb_param::kValLength);
    }
  }
}
