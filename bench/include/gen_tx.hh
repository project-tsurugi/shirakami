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

using namespace kvs;
using namespace ycsb_param;

/**
 * @brief generate search/update operations.
 */
static void
gen_tx_rw(std::vector<OprObj>& opr_set, std::size_t tpnm, std::size_t opnm, std::size_t rratio, Xoroshiro128Plus& rnd, FastZipf& zipf) {
  for (auto i = 0; i < opnm; ++i) {
    uint64_t keynm = zipf() % tpnm;
    uint64_t keybs = __builtin_bswap64(keynm);
    //std::unique_ptr<char[]> key = std::make_unique<char[]>(kKeyLength);
    //memcpy(key.get(), (std::to_string(keynm)).c_str(), kKeyLength);
    if ((rnd.next() % 100) < rratio) {
      opr_set.emplace_back(OP_TYPE::SEARCH, reinterpret_cast<char*>(&keybs), sizeof(uint64_t));
    } else {
      std::string value(kValLength, '0');
      make_string(value, rnd);
      opr_set.emplace_back(OP_TYPE::UPDATE, reinterpret_cast<char*>(&keybs), sizeof(uint64_t), value.data(), kValLength);
    }
  }
}
