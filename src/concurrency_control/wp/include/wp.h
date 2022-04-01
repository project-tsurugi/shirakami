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

#include "shirakami/scheme.h"

#include "glog/logging.h"

namespace shirakami::wp {

class wp_lock {
public:
    static bool is_locked(std::uint64_t obj) { return obj & 1; } // NOLINT

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
            std::uint64_t desired{expected | 1}; // NOLINT
            if (obj.compare_exchange_weak(expected, desired,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
                break;
            }
        }
    }

    void unlock() {
        std::uint64_t desired{obj.load(std::memory_order_acquire)};
        std::uint64_t locked_num{desired >> 1}; // NOLINT
        ++locked_num;
        desired = locked_num << 1; // NOLINT
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
    using wped_elem_type = std::pair<epoch::epoch_t, std::size_t>;
    using wped_type = std::array<wped_elem_type, WP_MAX_OVERLAP>;
    using wped_used_type = std::bitset<WP_MAX_OVERLAP>;

    wp_meta() { init(); }

    static bool empty(const wped_type& wped) {
        for (auto&& elem : wped) {
            if (elem != std::pair<epoch::epoch_t, std::size_t>(0, 0)) {
                return false;
            }
        }
        return true;
    }

    void clear_wped() {
        for (auto&& elem : wped_) { elem = {0, 0}; }
    }

    void display() {
        for (std::size_t i = 0; i < WP_MAX_OVERLAP; ++i) {
            if (get_wped_used().test(i)) {
                LOG(INFO) << "epoch:\t" << get_wped().at(i).first << ", id:\t"
                          << get_wped().at(i).second;
            }
        }
    }

    void init() {
        wped_ = {};
        wped_used_.reset();
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

    [[nodiscard]] const wped_type& get_wped() const { return wped_; }

    /**
     * @brief Get the wped_used_ object
     * @return std::bitset<WP_MAX_OVERLAP>& 
     */
    wped_used_type& get_wped_used() { return wped_used_; }

    [[nodiscard]] const wped_used_type& get_wped_used() const {
        return wped_used_;
    }

    /**
     * @brief check the space of write preserve.
     * @param[out] at If this function returns Status::OK, the value of @a at 
     * shows empty slot.
     * @return Status::OK success.
     * @return Status::WARN_NOT_FOUND fail.
     */
    Status find_slot(std::size_t& at) {
        for (std::size_t i = 0; i < WP_MAX_OVERLAP; ++i) {
            if (!wped_used_.test(i)) {
                at = i;
                return Status::OK;
            }
        }
        return Status::WARN_NOT_FOUND;
    }

    static epoch::epoch_t find_min_ep(const wped_type& wped) {
        bool first{true};
        epoch::epoch_t min_ep{0};
        for (auto&& elem : wped) {
            if (elem.first != 0) {
                // used slot
                if (first) {
                    first = false;
                    min_ep = elem.first;
                } else if (min_ep > elem.first) {
                    min_ep = elem.first;
                }
            }
        }
        return min_ep;
    }

    static std::size_t find_min_id(const wped_type& wped) {
        bool first{true};
        std::size_t min_id{0};
        for (auto&& elem : wped) {
            if (elem.first != 0) {
                // used slot
                if (first) {
                    first = false;
                    min_id = elem.second;
                } else if (min_id > elem.second) {
                    min_id = elem.second;
                }
            }
        }
        return min_id;
    }

    /**
     * @brief single register.
     */
    Status register_wp(epoch::epoch_t ep, std::size_t id) {
        wp_lock_.lock();
        std::size_t slot{};
        if (Status::OK != find_slot(slot)) { return Status::ERR_FAIL_WP; }
        wped_used_.set(slot);
        set_wped(slot, {ep, id});
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
        for (std::size_t i = 0; i < WP_MAX_OVERLAP; ++i) {
            if (wped_.at(i).second == id) {
                set_wped(i, {0, 0});
                wped_used_.reset(i);
                wp_lock_.unlock();
                return Status::OK;
            }
        }
        return Status::WARN_NOT_FOUND;
    }

    void set_wped(std::size_t const pos,
                  wped_elem_type const val) {
        wped_.at(pos) = val;
    }

    void set_wped_used(std::size_t pos, bool val = true) { // NOLINT
        wped_used_.set(pos, val);
    }

private:
    /**
     * @brief write preserve infomation.
     * @details first of each vector's element is epoch which is the valid 
     * point of wp. second of those is the batch id. 
     */
    wped_type wped_;

    /**
     * @brief Represents a used slot in wped_.
     */
    wped_used_type wped_used_;

    /**
     * @brief mutex for wped_
     */
    wp_lock wp_lock_;
};

class page_set_meta {
public:
    point_read_by_bt* get_point_read_by_ptr() { return &point_read_by_; }

    range_read_by_bt* get_range_read_by_ptr() { return &range_read_by_; }

    wp_meta* get_wp_meta_ptr() { return &wp_meta_; }

private:
    point_read_by_bt point_read_by_;
    range_read_by_bt range_read_by_;
    wp_meta wp_meta_;
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
 * @brief termination process about wp.
 */
[[maybe_unused]] extern Status fin();

/**
 * @brief There is no metadata that should be there.
 */
[[maybe_unused]] extern wp_meta::wped_type find_wp(Storage storage);

[[maybe_unused]] extern Status find_page_set_meta(Storage st,
                                                  page_set_meta*& ret);

[[maybe_unused]] extern Status find_read_by(Storage st, range_read_by_bt*& ret);

[[maybe_unused]] extern Status find_read_by(Storage st, point_read_by_bt*& ret);

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
                                              std::size_t batch_id,
                                              epoch::epoch_t valid_epoch);

} // namespace shirakami::wp