/**
 * @file session.h
 */

#pragma once

#include "cpu.h"
#include "epoch.h"

#include <atomic>

namespace shirakami {

class alignas(CACHE_LINE_SIZE) session {
public:

private:
    /**
     * @brief most recently chosen tid for calculate new tid.
     */
    tid_word mrc_tid_{};

    /**
     * @brief If this is true, this session is live, otherwise, not live.
     */
    std::atomic<bool> visible_{false};

    /**
     * @brief Flag of transaction beginning.
     * @details If this is true, this session is in some tx, otherwise, not.
     */
    std::atomic<bool> tx_began_{false};

}; // namespace shirakami