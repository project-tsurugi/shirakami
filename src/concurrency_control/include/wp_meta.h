#pragma once

#include <array>
#include <bitset>
#include <cstddef>
#include <map>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "cpu.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/wp_lock.h"

#include "shirakami/scheme.h"

namespace shirakami::wp {

/**
 * @brief metadata about wp attached to each table (page sets).
 * @details
 */
class alignas(CACHE_LINE_SIZE) wp_meta {
public:

    wp_meta() { init(); }

    void display();

    void init();

    wp_lock& get_wp_lock() { return wp_lock_; }

    // ==========

private:

    /**
     * @brief mutex for wped_
     */
    wp_lock wp_lock_;

};

} // namespace shirakami::wp
