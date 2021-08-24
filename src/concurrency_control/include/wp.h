/**
 * @file wp.h
 * @brief header about write preserve
 */

#pragma once

#include "cpu.h"

#include "shirakami/scheme.h"

#include <mutex>
#include <shared_mutex>
#include <vector>

namespace shirakami {

inline std::mutex wp_mutex;

[[maybe_unused]] static std::unique_lock<std::mutex> get_wp_mutex() {
    return std::unique_lock<std::mutex>{wp_mutex};
}

/**
 * @brief metadata about wp attached to each table (page sets).
 * @details
 */
class alignas(CACHE_LINE_SIZE) wp_meta {
public:
    using wped_type = std::vector<std::pair<std::size_t, std::size_t>>;

    wped_type get_wped() {
        std::shared_lock sh_lock{wped_mtx_};
        return wped_;
    }

    void register_wp(std::size_t epoc, std::size_t id) {
        std::unique_lock u_lock{wped_mtx_};
        wped_.emplace_back(epoc, id);
    }

    /**
     * @brief remove element from wped_
     * @param[in] id batch id.
     * @return Status::OK success.
     * @return Status::WARN_NOT_FOUND fail.
     */
    Status remove_wp(std::size_t id) {
        std::unique_lock u_lock{wped_mtx_};
        for (auto it = wped_.begin(); it != wped_.end();) {
            if ((*it).second == id) {
                wped_.erase(it);
                return Status::OK;
            } else {
                ++it;
            }
        }
        return Status::WARN_NOT_FOUND;
    }

    std::size_t size_wp() {
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

} // namespace shirakami