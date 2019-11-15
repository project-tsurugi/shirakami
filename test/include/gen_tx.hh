/**
 * @file
 * @brief generate operations of transaction.
 */

#pragma once

#include "string.hh"
#include "ycsb_param.h"

// kvs_charkey/src/
#include "include/random.hh"
#include "include/scheme.h"
#include "include/zipf.hh"

using namespace kvs;
using namespace ycsb_param;

/**
 * @brief generate search/update operations.
 */
static void
gen_tx_rw(std::vector<OprObj>& opr_set, std::size_t tpnm, std::size_t opnm, std::size_t rratio, Xoroshiro128Plus& rnd, FastZipf& zipf) {
  for (auto i = 0; i < opnm; ++i) {
    uint64_t keynm = zipf() % tpnm;
    std::unique_ptr<char[]> key = std::make_unique<char[]>(kKeyLength);
    memcpy(key.get(), (std::to_string(keynm)).c_str(), kKeyLength);
    if ((rnd.next() % 100) < rratio) {
      opr_set.emplace_back(SEARCH, key.get(), kKeyLength);
    } else {
      std::unique_ptr<char[]> val = std::make_unique<char[]>(kValLength);
      make_string(val.get(), kValLength);
      opr_set.emplace_back(UPDATE, key.get(), kKeyLength, val.get(), kValLength);
    }
  }
}
