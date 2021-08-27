/**
 * @file concurrency_control/wp/include/record.h
 * @brief header about record
 */

#pragma once

#include "concurrency_control/silo/include/tid.h"

#include "cpu.h"
#include "version.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) Record { // NOLINT
public:
    Record() {} // NOLINT

private:
    tid_word tid_;
    /**
     * @brief Pointer to latest version
     * @details The version infomation which it should have at each version.
     */
    std::atomic<version*> latest_;
};

} // namespace shirakami
