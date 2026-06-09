#pragma once

#include <map>
#include <shared_mutex>
#include <tuple>
#include <variant>

#include "shirakami/logging.h"
#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

class scan_handler;

class alignas(CACHE_LINE_SIZE) scan_context_vscan {
public:
    // getter
    auto& get_vec() { return vec_; }

    // setter

private:
    std::vector<std::tuple<const Record*,
                           yakushima::node_version64_body,
                           yakushima::node_version64*>> vec_{};
};

class alignas(CACHE_LINE_SIZE) scan_context_iscan {
public:
    scan_context_iscan() = default;
    scan_context_iscan(const scan_context_iscan&) = delete;
    scan_context_iscan(scan_context_iscan&&) = delete;
    scan_context_iscan& operator=(const scan_context_iscan&) = delete;
    scan_context_iscan& operator=(scan_context_iscan&&) = delete;

    ~scan_context_iscan() {
        if (ycontext_) { yakushima::iscan_close(ycontext_); }
    }

    // getter
    [[nodiscard]] Status get_error() const { return error_; }
    [[nodiscard]] std::size_t get_max_size() const { return max_size_; }
    yakushima::iscan_context*& get_ycontext_ref() { return ycontext_; }
    Record*& get_rec_ptr_ref() { return rec_ptr_; }

    // setter
    void set_error(Status error) { error_ = error; }
    void set_max_size(std::size_t max_size) { max_size_ = max_size; }

private:
    yakushima::iscan_context* ycontext_{nullptr};
    Record* rec_ptr_{};
    std::size_t max_size_{0U}; // deprecated

    /**
     * @brief remember error occurred in next() until read_from_scan() call
     * @details to emulate when vector-based scan returns an error.
     */
    Status error_{Status::OK};
};

class alignas(CACHE_LINE_SIZE) scan_context {
public:
    explicit scan_context(std::in_place_type_t<scan_context_vscan>) : var_(std::in_place_type<scan_context_vscan>) {}
    explicit scan_context(std::in_place_type_t<scan_context_iscan>) : var_(std::in_place_type<scan_context_iscan>) {}

    // getter
    [[nodiscard]] Storage get_storage() const { return storage_; }
    [[nodiscard]] std::size_t get_scan_index() const { return scan_index_; }
    std::size_t& get_scan_index_ref() { return scan_index_; }
    [[nodiscard]] std::string_view get_r_key() const { return r_key_; }
    [[nodiscard]] scan_endpoint get_r_end() const { return r_end_; }
    [[nodiscard]] scan_handler* get_parent() const { return parent_; }
    [[nodiscard]] bool is_iscan() const { return std::holds_alternative<scan_context_iscan>(var_); }
    scan_context_vscan& get_context_vscan_ref() { return std::get<scan_context_vscan>(var_); }
    scan_context_iscan& get_context_iscan_ref() { return std::get<scan_context_iscan>(var_); }

    // setter
    void set_storage(Storage storage) { storage_ = storage; }
    void set_r_key(std::string_view r_key) { r_key_ = r_key; }
    void set_r_end(scan_endpoint r_end) { r_end_ = r_end; }
    void set_parent(scan_handler* parent) { parent_ = parent; }

private:
    Storage storage_{};
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

    std::variant<scan_context_vscan, scan_context_iscan> var_;
};

class scan_handler {
public:
    void clear() {
        std::lock_guard lk{mtx_allocated_}; // for strand
        for (auto itr = allocated_.begin(); itr != allocated_.end(); ) {
            itr = allocated_.erase(itr);
        }
    }

    Status delete_scan_context(scan_context* sc) {
        {
            std::lock_guard lk{mtx_allocated_}; // for strand
            auto itr = allocated_.find(sc);
            if (itr == allocated_.end()) {
                return Status::WARN_INVALID_HANDLE;
            }
            allocated_.erase(itr);
        }

        return Status::OK;
    }

    template<typename T>
    scan_context* create_scan_context(std::in_place_type_t<T> tag) {
        auto* sc = new scan_context(tag); // NOLINT
        sc->set_parent(this);
        std::lock_guard lk{mtx_allocated_};
        allocated_.insert(std::unique_ptr<scan_context>{sc});
        return sc;
    }

    // for shirakami user code debugging
    // note: precise-handle-check may cause concurrent contentions problems
    static constexpr bool precise_handle_check = false;

    Status check_valid_scan_handle(scan_context* sc) {
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

    /// @brief set of scan context allocated from this handler
    std::set<std::unique_ptr<scan_context>, less_uniqp_rawp<scan_context>> allocated_{};

    /**
     * @brief mutex for allocated set
     */
    std::mutex mtx_allocated_;
};

// mode flag

inline bool scan_mode_iterator_based_{true};

static inline bool get_scan_mode_iterator_based() {
    return scan_mode_iterator_based_;
}

static inline void set_scan_mode_iterator_based(bool b) {
    scan_mode_iterator_based_ = b;
}

} // namespace shirakami
