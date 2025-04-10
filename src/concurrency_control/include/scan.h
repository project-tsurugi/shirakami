#pragma once

#include <map>
#include <shared_mutex>
#include <tuple>

#include "shirakami/logging.h"
#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

class scan_handler;

class alignas(CACHE_LINE_SIZE) scan_cache_obj {
public:
    // getter
    [[nodiscard]] Storage get_storage() const { return storage_; }
    auto& get_vec() { return vec_; }
    [[nodiscard]] std::size_t get_scan_index() const { return scan_index_; }
    std::size_t& get_scan_index_ref() { return scan_index_; }
    [[nodiscard]] std::string_view get_r_key() const { return r_key_; }
    [[nodiscard]] scan_endpoint get_r_end() const { return r_end_; }
    [[nodiscard]] scan_handler* get_parent() const { return parent_; }

    // setter
    void set_storage(Storage storage) { storage_ = storage; }
    void set_r_key(std::string_view r_key) { r_key_ = r_key; }
    void set_r_end(scan_endpoint r_end) { r_end_ = r_end; }
    void set_parent(scan_handler* parent) { parent_ = parent; }

private:
    Storage storage_{};
    std::vector<std::tuple<const Record*,
                           yakushima::node_version64_body,
                           yakushima::node_version64*>> vec_{};
    std::size_t scan_index_{0U};

    /**
     * @brief range of right endpoint for ltx
     * @details if user read to right endpoint till scan limit, shirakami needs
     * to know this information to log range info.
     */
    std::string r_key_{};

    scan_endpoint r_end_{};

    /**
     * @brief scan_handler that allocated this
     */
    scan_handler* parent_{};
};

class scan_handler {
public:
    void clear() {
        // for strand
        std::lock_guard lk{mtx_allocated_};
        for (auto it = allocated_.begin(); it != allocated_.end(); ) {
            it = allocated_.erase(it);
        }
    }

    Status delete_scan_cache(scan_cache_obj* sc) {
        {
            // for strand
            std::lock_guard lk{mtx_allocated_};
            auto itr = allocated_.find(sc);
            if (itr == allocated_.end()) {
                return Status::WARN_INVALID_HANDLE;
            }
            allocated_.erase(itr);
        }

        return Status::OK;
    }

    scan_cache_obj* create_scan_cache() {
        auto* sc = new scan_cache_obj(); // NOLINT
        sc->set_parent(this);
        std::lock_guard lk{mtx_allocated_};
        allocated_.insert(std::unique_ptr<scan_cache_obj>{sc});
        return sc;
    }

    // for shirakami user code debugging
    // note: precise-handle-check may cause concurrent contentions problems
    static constexpr bool precise_handle_check = false;

    Status check_valid_scan_handle(scan_cache_obj* sc) {
        if constexpr (precise_handle_check) {
            // for strand
            std::lock_guard lk{mtx_allocated_};
            if (allocated_.find(sc) == allocated_.end()) {
                return Status::WARN_INVALID_HANDLE;
            }
        } else {
            if (sc == nullptr || sc->get_parent() != this) {
                return Status::WARN_INVALID_HANDLE;
            }
        }
        return Status::OK;
    }

private:

    template<typename T>
    struct less_uniqp_rawp {
        using is_transparent = void;
        using UP = const std::unique_ptr<T>&;
        using RP = T*const&;
        bool operator()(UP a, RP b) const { return a.get() < b; }
        bool operator()(RP a, UP b) const { return a < b.get(); }
        bool operator()(UP a, UP b) const { return a.get() < b.get(); }
    };

    /**
     * @brief set of scan cache objects allocated from this handler
     */
    std::set<std::unique_ptr<scan_cache_obj>, less_uniqp_rawp<scan_cache_obj>> allocated_{};

    /**
     * @brief mutex for allocated set
     */
    std::mutex mtx_allocated_;
};

} // namespace shirakami
