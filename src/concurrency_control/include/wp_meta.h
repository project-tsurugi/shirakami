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
     * @brief First is the epoch of the tx. Second is the id of the tx.
     */
    using wped_elem_type = std::pair<epoch::epoch_t, std::size_t>;
    using wped_type = std::array<wped_elem_type, KVS_MAX_PARALLEL_THREADS>;
    /**
     * key: tx id
     * value: whether write, left key, right key
     */
    using wp_write_range_type =
            std::map<std::size_t, std::tuple<std::string, std::string>>;

    wp_meta() { init(); }

    static bool empty(const wped_type& wped);

    void clear_wped() {
        for (auto&& elem : wped_) { elem = {0, 0}; }
    }

    void display();

    void init();

    wp_lock& get_wp_lock() { return wp_lock_; }

    std::shared_mutex& get_mtx_write_range() { return mtx_write_range_; }

    wp_write_range_type& get_write_range() { return write_range_; }

    // ==========

private:
    /**
     * @brief write preserve infomation.
     * @details first of each vector's element is epoch which is the valid
     * point of wp. second of those is the long tx's id.
     */
    wped_type wped_;

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
