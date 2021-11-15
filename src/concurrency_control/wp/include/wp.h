/**
 * @file concurrency_control/wp/include/wp.h
 * @brief header about write preserve
 */

#pragma once

#include "cpu.h"

#include "concurrency_control/wp/include/session.h"

#include "shirakami/scheme.h"

#include <mutex>
#include <shared_mutex>
#include <vector>

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

/**
 * @brief metadata about wp attached to each table (page sets).
 * @details
 */
class alignas(CACHE_LINE_SIZE) wp_meta {
public:
    using wped_type = std::vector<std::pair<std::size_t, std::size_t>>;

    void clear_wped() { wped_.clear(); }

    wped_type get_wped() {
        std::shared_lock sh_lock{wped_mtx_};
        return wped_;
    }

    /**
     * @brief single register.
     */
    Status register_wp(std::size_t epoc, std::size_t id) {
        std::unique_lock u_lock{wped_mtx_};
        if (wped_.empty()) {
            wped_.emplace_back(epoc, id);
            return Status::OK;
        }
        /**
         * Since the overlapping of wp is complicated, it is optimized or used 
         * as wp-k.
         */
        return Status::ERR_FAIL_WP;
    }

    /**
     * @brief batch register.
     */
    void
    register_wp(std::vector<std::pair<std::size_t, std::size_t>> const& wps) {
        std::unique_lock u_lock{wped_mtx_};
        for (auto&& elem : wps) { wped_.emplace_back(elem.first, elem.second); }
    }

    /**
     * @brief remove element from wped_
     * @param[in] id batch id.
     * @return Status::OK success.
     * @return Status::WARN_NOT_FOUND fail.
     */
    [[nodiscard]] Status remove_wp(std::size_t const id) {
        std::unique_lock u_lock{wped_mtx_};
        for (auto it = wped_.begin(); it != wped_.end();) {
            if ((*it).second == id) {
                wped_.erase(it);
                return Status::OK;
            }
            ++it;
        }
        return Status::WARN_NOT_FOUND;
    }

    [[nodiscard]] std::size_t size_wp() {
        std::shared_lock sh_lock{wped_mtx_};
        return wped_.size();
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
    std::shared_mutex wped_mtx_;
};

} // namespace shirakami::wp