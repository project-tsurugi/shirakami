#pragma once

#include <map>
#include <shared_mutex>
#include <tuple>

#include "shirakami/logging.h"
#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

class scanned_storage_set {
public:
    Storage get(ScanHandle const hd) {  // NOLINT(misc-misplaced-const)
        std::shared_lock<std::shared_mutex> lk{get_mtx()};
        return map_[hd];
    }

    void clear() {
        std::lock_guard<std::shared_mutex> lk{get_mtx()};
        map_.clear();
    }

    void clear(ScanHandle const hd) { // NOLINT(misc-misplaced-const)
        // for strand
        std::lock_guard<std::shared_mutex> lk{get_mtx()};
        map_.erase(hd);
    }

    void set(ScanHandle const hd, Storage const st) { // NOLINT(misc-misplaced-const)
        std::lock_guard<std::shared_mutex> lk{get_mtx()};
        map_[hd] = st;
    };

    std::shared_mutex& get_mtx() { return mtx_; }

private:
    std::map<ScanHandle, Storage> map_;

    /**
     * @brief mutex for scanned storage set
     */
    std::shared_mutex mtx_{};
};

class scan_handler {
public:
    class scan_cache_obj {
    private:
        Storage storage_{};
        std::vector<std::tuple<const Record*,
                               yakushima::node_version64_body,
                               yakushima::node_version64*>> vec_;
        std::size_t itr_{0U};
    public:
        //[[nodiscard]] Storage get_storage() const { return storage_; }
        decltype(vec_)& get_vec() { return vec_; }
        decltype(itr_)& get_itr() { return itr_; }
        void set_storage(Storage storage) { storage_ = storage; }
        //void set_vec(decltype(vec_) vec) { vec_ = vec; }
        //void set_itr(decltype(itr_) itr) { itr_ = itr; }
    };
    class scan_cache_dummy {
    // dummy class for transition
    public:
        void clear() {
            std::lock_guard lk{allocated_mtx};
            for (auto it = allocated.begin(); it != allocated.end(); ) {
                delete *it; // NOLINT
                it = allocated.erase(it);
            }
        }
        scan_cache_obj* find(ScanHandle sh) { return static_cast<scan_cache_obj*>(sh); } // NOLINT(readability-convert-member-functions-to-static)
        scan_cache_obj* end() { return nullptr; } // NOLINT(readability-convert-member-functions-to-static)
        void erase(scan_cache_obj* o) {
            std::lock_guard lk{allocated_mtx};
            allocated.erase(o);
            delete o; // NOLINT
        }
        scan_cache_obj& operator[](ScanHandle sh) {return *find(sh);}

        ScanHandle allocate() {
            auto* n = new scan_cache_obj(); // NOLINT
            std::lock_guard lk{allocated_mtx};
            allocated.insert(n);
            return n;
        }

        // disable copy
        scan_cache_dummy(const scan_cache_dummy&) = delete;
        scan_cache_dummy& operator=(const scan_cache_dummy&) = delete;
        scan_cache_dummy(scan_cache_dummy&&) = delete;
        scan_cache_dummy& operator=(scan_cache_dummy&&) = delete;
        scan_cache_dummy() = default;
        ~scan_cache_dummy() = default;

    private:
        std::mutex allocated_mtx;
        std::set<scan_cache_obj*> allocated{};
    };
    using scan_cache_type = scan_cache_dummy;

    void clear() {
        {
            // for strand
            //std::lock_guard<std::shared_mutex> lk{get_mtx_scan_cache()};
            get_scan_cache().clear();
        }
        get_scanned_storage_set().clear();
    }

    Status clear(ScanHandle hd) {
        // about scan cache
        {
            // for strand
            //std::lock_guard<std::shared_mutex> lk{get_mtx_scan_cache()};
            auto* itr = get_scan_cache().find(hd);
            if (itr == get_scan_cache().end()) {
                return Status::WARN_INVALID_HANDLE;
            }
            get_scan_cache().erase(itr);

            // about scan cache iterator
            set_r_key("");
            set_r_end(scan_endpoint::EXCLUSIVE);
        }

        // about scanned storage set
        scanned_storage_set_.clear(hd);

        return Status::OK;
    }

    // getter

    [[maybe_unused]] scan_cache_type& get_scan_cache() { // NOLINT
        return scan_cache_;
    }

    //std::shared_mutex& get_mtx_scan_cache() { return mtx_scan_cache_; }

    scanned_storage_set& get_scanned_storage_set() {
        return scanned_storage_set_;
    }

    [[nodiscard]] std::string_view get_r_key() const { return r_key_; }

    [[nodiscard]] scan_endpoint get_r_end() const { return r_end_; }

    // setter

    void set_r_key(std::string_view r_key) { r_key_ = r_key; }

    void set_r_end(scan_endpoint r_end) { r_end_ = r_end; }

private:
    /**
     * @brief cache of index scan.
     */
    scan_cache_type scan_cache_{};

    /**
     * @brief mutex for scan cache
     */
    //std::shared_mutex mtx_scan_cache_{};

    /**
     * @brief scanned storage set.
     * @details As a result of being scanned, the pointer to the record
     * is retained. However, it does not retain the scanned storage information
     * . Without it, you will have problems generating read sets.
     */
    scanned_storage_set scanned_storage_set_{};

    /**
     * @brief range of right endpoint for ltx
     * @details if user read to right endpoint till scan limit, shirakami needs
     * to know this information to log range info.
     */
    std::string r_key_{};

    scan_endpoint r_end_{};
};

} // namespace shirakami
