/**
 * @file concurrency_control/wp/include/wp.h
 * @brief header about write preserve
 */

#pragma once

#include <xmmintrin.h>

#include <atomic>
#include <bitset>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "cpu.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/read_by.h"
#include "concurrency_control/wp/include/read_plan.h"
#include "concurrency_control/wp/include/wp_lock.h"
#include "concurrency_control/wp/include/wp_meta.h"

#include "shirakami/storage_options.h"

#include "glog/logging.h"

namespace shirakami {

// forward declaration
class session;
class read_plan;

namespace wp {

class page_set_meta {
public:
    point_read_by_long* get_point_read_by_long_ptr() {
        return &point_read_by_long_;
    }

    range_read_by_long* get_range_read_by_long_ptr() {
        return &range_read_by_long_;
    }

    range_read_by_short* get_range_read_by_short_ptr() {
        return &range_read_by_short_;
    }

    read_plan& get_read_plan() { return read_plan_; }

    wp_meta* get_wp_meta_ptr() { return &wp_meta_; }

private:
    point_read_by_long point_read_by_long_;
    range_read_by_long range_read_by_long_;
    range_read_by_short range_read_by_short_;
    wp_meta wp_meta_;
    read_plan read_plan_;
};

constexpr Storage initial_page_set_meta_storage{};

/**
 * @brief whether it is finalizing about wp.
 */
inline std::atomic<bool> finalizing{false};

/**
 * @brief whether it was initialized about wp.
 */
inline bool initialized{false};


/**
 * @brief The mutex excluding fetch_add  from batch_counter and executing wp.
 */
inline std::mutex wp_mutex;

inline Storage page_set_meta_storage{initial_page_set_meta_storage};

/**
 * @brief extract ltxs info higher priority than @a ltx_id from @a wps.
 * @param[in] ti The transaction which executes on this session takes information.
 * @param[in] wp_meta_ptr wp information.
 * @param[in] wps wp information.
 */
void extract_higher_priori_ltx_info(session* ti, wp_meta* wp_meta_ptr,
                                    wp_meta::wped_type const& wps);

/**
 * @brief termination process about wp.
 */
[[maybe_unused]] extern Status fin();

/**
 * @brief There is no metadata that should be there.
 */
[[maybe_unused]] extern wp_meta::wped_type find_wp(Storage storage);

[[maybe_unused]] extern Status find_page_set_meta(Storage st,
                                                  page_set_meta*& ret);

[[maybe_unused]] extern Status find_read_by(Storage st,
                                            range_read_by_long*& ret);

[[maybe_unused]] extern Status find_read_by(Storage st,
                                            point_read_by_long*& ret);

[[maybe_unused]] extern Status find_wp_meta(Storage st, wp_meta*& ret);

/**
 * @brief getter
 */
[[maybe_unused]] static bool get_finalizing() {
    return finalizing.load(std::memory_order_acquire);
}

/**
 * @brief getter
 */
[[maybe_unused]] static bool get_initialized() { return initialized; }

/**
 * @brief getter
 */
[[maybe_unused]] static Storage get_page_set_meta_storage() {
    return page_set_meta_storage;
}

/**
 * @brief getter.
 */
[[maybe_unused]] static std::mutex& get_wp_mutex() { return wp_mutex; }

/**
 * @brief initialization about wp.
 * @return Status::OK success.
 */
[[maybe_unused]] extern Status init();

/**
 * @brief setter.
 */
[[maybe_unused]] static void set_finalizing(bool tf) {
    finalizing.store(tf, std::memory_order_release);
}

/**
 * @brief setter.
 */
[[maybe_unused]] static void set_initialized(bool tf) { initialized = tf; }

/**
 * @brief setter.
 */
[[maybe_unused]] static void set_page_set_meta_storage(Storage storage) {
    page_set_meta_storage = storage;
}

[[maybe_unused]] extern Status write_preserve(Token token,
                                              std::vector<Storage> storage,
                                              std::size_t long_tx_id,
                                              epoch::epoch_t valid_epoch);

} // namespace wp

} // namespace shirakami