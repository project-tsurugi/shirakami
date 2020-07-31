/**
 * @file thread_info_table.h
 * @brief core work about shirakami.
 */

#pragma once

#include <array>

#include "thread_info.h"

namespace shirakami::cc_silo_variant {

class thread_info_table {
public:
  /**
   * @brief Check wheter the session is already started. This function is not
   * thread safe. But this function can be used only after taking mutex.
   */
  static Status decide_token(Token& token);  // NOLINT

  /**
   * @brief fin work about kThreadTable
   */
  static void fin_kThreadTable();

  static std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS>&
  get_thread_info_table() {  // NOLINT
    return kThreadTable;
  }

  /**
   * @brief init work about kThreadTable
   */
  static void init_kThreadTable();

private:
  static inline std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS>  // NOLINT
      kThreadTable;                                               // NOLINT
};

}  // namespace shirakami::silo_variant
