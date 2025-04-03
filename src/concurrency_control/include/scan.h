#pragma once

#include <map>
#include <shared_mutex>
#include <tuple>

#include "shirakami/logging.h"
#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) scan_cache_obj {
public:
    Storage& get_storage() { return storage_; }
    auto& get_vec() { return vec_; }
    std::size_t& get_scan_index() { return scan_index_; }

    // getter
    [[nodiscard]] std::string_view get_r_key() const { return r_key_; }
    [[nodiscard]] scan_endpoint get_r_end() const { return r_end_; }

    // setter
    void set_r_key(std::string_view r_key) { r_key_ = r_key; }
    void set_r_end(scan_endpoint r_end) { r_end_ = r_end; }

private:
    Storage storage_{};
    std::vector<std::tuple<const Record*,
                           yakushima::node_version64_body,
                           yakushima::node_version64*>> vec_;
    std::size_t scan_index_{};

    /**
     * @brief range of right endpoint for ltx
     * @details if user read to right endpoint till scan limit, shirakami needs
     * to know this information to log range info.
     */
    std::string r_key_{};

    scan_endpoint r_end_{};
};

class scan_handler {
public:
    void clear() {
        {
            // for strand
            //std::lock_guard<std::shared_mutex> lk{get_mtx_scan_cache()};
            std::lock_guard lk{mtx_allocated_};
            for (auto it = allocated_.begin(); it != allocated_.end(); ) {
                delete *it; // NOLINT
                it = allocated_.erase(it);
            }
        }
    }

    Status clear(scan_cache_obj* sc) {
        {
            // for strand
            //std::lock_guard<std::shared_mutex> lk{get_mtx_scan_cache()};
            if (check_valid_scan_handle(sc) != Status::OK) {
                return Status::WARN_INVALID_HANDLE;
            }
            {
                std::lock_guard lk{mtx_allocated_};
                allocated_.erase(sc);
                delete sc; // NOLINT
            }
        }

        return Status::OK;
    }

    scan_cache_obj* allocate() {
        auto* sc = new scan_cache_obj(); // NOLINT
        std::lock_guard lk{mtx_allocated_};
        allocated_.insert(sc);
        return sc;
    }

    static constexpr bool precise_handle_check = false;

    Status check_valid_scan_handle(scan_cache_obj* sc) {
        if constexpr (precise_handle_check) {
            // for strand
            std::lock_guard lk{mtx_allocated_};
            if (allocated_.find(sc) == allocated_.end()) {
                return Status::WARN_INVALID_HANDLE;
            }
        } else {
            if (sc == nullptr) {
                return Status::WARN_INVALID_HANDLE;
            }
        }
        return Status::OK;
    }

private:

    /**
     * @brief set of scan cache objects allocated from this handler
     */
    std::set<scan_cache_obj*> allocated_{};

    /**
     * @brief mutex for allocated set
     */
    std::mutex mtx_allocated_;
};

} // namespace shirakami
