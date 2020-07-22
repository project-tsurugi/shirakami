/**
 * @file concurrency_control.h
 */

#pragma once

#include "thread_info.h"

namespace shirakami {

class cc_silo {
public:
  static void write_phase(ThreadInfo* ti, const tid_word& max_rset,
                          const tid_word& max_wset);
};

}  // namespace shirakami
