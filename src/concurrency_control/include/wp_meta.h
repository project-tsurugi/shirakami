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
    /**
     * key: tx id
     * value: whether write, left key, right key
     */
    using wp_write_range_type =
            std::map<std::size_t, std::tuple<std::string, std::string>>;

    wp_meta() { init(); }

    void display();

    void init();

    wp_lock& get_wp_lock() { return wp_lock_; }

    std::shared_mutex& get_mtx_write_range() { return mtx_write_range_; }

    wp_write_range_type& get_write_range() { return write_range_; }

    // ==========

private:

    /**
     * @brief mutex for wped_
     */
    wp_lock wp_lock_;

    // about write range
    // ==========
    std::shared_mutex mtx_write_range_;

    wp_write_range_type write_range_;
    // ==========
};

} // namespace shirakami::wp
