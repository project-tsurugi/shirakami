/**
 * @file concurrency_control/wp/include/wp.h
 * @brief header about write preserve
 */

#pragma once

#include <xmmintrin.h>

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "cpu.h"

#include "concurrency_control/wp/include/session.h"

#include "shirakami/scheme.h"

namespace shirakami::wp {

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
 * @brief termination process about wp.
 */
[[maybe_unused]] extern Status fin();

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
 * @return Status::ERR_STORAGE error about storage.
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

[[maybe_unused]] extern Status write_preserve(session* ti,
                                              std::vector<Storage> storage,
                                              std::size_t batch_id,
                                              epoch::epoch_t valid_epoch);

class wp_lock {
public:
    static bool is_locked(std::uint64_t obj) { return obj & 1; }

    std::uint64_t load_obj() { return obj.load(std::memory_order_acquire); }

    void lock() {
        std::uint64_t expected{obj.load(std::memory_order_acquire)};
        for (;;) {
            if (is_locked(expected)) {
                // locked by others
                _mm_pause();
                expected = obj.load(std::memory_order_acquire);
                continue;
            }
            std::uint64_t desired{expected | 1};
            if (obj.compare_exchange_weak(expected, desired,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
                break;
            }
        }
    }

    void unlock() {
        std::uint64_t desired{obj.load(std::memory_order_acquire)};
        std::uint64_t locked_num{desired >> 1};
        ++locked_num;
        desired = locked_num << 1;
        obj.store(desired, std::memory_order_release);
    }

private:
    /**
     * @brief lock object
     * @details One bit indicates the presence or absence of a lock. 
     * The rest represents the number of past locks.
     * The target object loaded while loading the unlocked object twice can be regarded as atomically loaded.
     */
    std::atomic<std::uint64_t> obj{0};
};

/**
 * @brief metadata about wp attached to each table (page sets).
 * @details
 */
class alignas(CACHE_LINE_SIZE) wp_meta {
public:
    using wped_type =
            std::array<std::pair<std::size_t, std::size_t>, WP_MAX_OVERLAP>;

    static bool empty(const wped_type& wped) {
        for (auto&& elem : wped) {
            if (elem != std::make_pair<std::size_t, std::size_t>(0, 0)) {
                return false;
            }
        }
        return true;
    }

    void clear_wped() {
        for (auto&& elem : wped_) { elem = {0, 0}; }
    }

    wped_type get_wped() {
        wped_type r_obj{};
        for (;;) {
            auto ts_f{wp_lock_.load_obj()};
            if (wp_lock::is_locked(ts_f)) {
                _mm_pause();
                continue;
            }
            r_obj = wped_;
            auto ts_s{wp_lock_.load_obj()};
            if (wp_lock::is_locked(ts_s) || ts_f != ts_s) { continue; }
            break;
        }
        return r_obj;
    }

    /**
     * @brief single register.
     */
    Status register_wp(std::size_t epoc, std::size_t id) {
        wp_lock_.lock();
        for (auto&& elem : wped_) {
            if (elem != std::make_pair<std::size_t, std::size_t>(0, 0)) {
                /**
                 * Since the overlapping of wp is complicated, it is optimized or used 
                 * as wp-k.
                 */
                wp_lock_.unlock();
                return Status::ERR_FAIL_WP;
            }
        }
        wped_.at(0) = {epoc, id};
        wp_lock_.unlock();
        return Status::OK;
    }

    /**
     * @brief remove element from wped_
     * @param[in] id batch id.
     * @return Status::OK success.
     * @return Status::WARN_NOT_FOUND fail.
     */
    [[nodiscard]] Status remove_wp(std::size_t const id) {
        wp_lock_.lock();
        for (auto&& elem : wped_) {
            if (elem.second == id) {
                elem = {0, 0};
                wp_lock_.unlock();
                return Status::OK;
            }
        }
        wp_lock_.unlock();
        return Status::WARN_NOT_FOUND;
    }

private:
    /**
     * @brief write preserve infomation.
     * @details first of each vector's element is epoch which is the valid point of wp.
     * second of those is the batch id. 
     */
    wped_type wped_;

    /**
     * @brief mutex for wped_
     */
    wp_lock wp_lock_;
};

} // namespace shirakami::wp