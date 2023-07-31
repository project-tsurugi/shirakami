#pragma once

#include <map>
#include <shared_mutex>
#include <vector>

#include "shirakami/scheme.h"
#include "shirakami/transaction_options.h"

namespace shirakami {

class session;

/**
 * @brief read plan
 * @details This is had at page set metadata. This object shows what transactions
 * read or not read the page set.
 */
class read_plan {
public:
    using read_area_type = transaction_options::read_area;
    using cont_type = std::map<std::size_t, read_area_type>;

    static void clear() {
        std::lock_guard<std::shared_mutex> lk{get_mtx_cont()};
        get_cont().clear();
    }
    static void init() {
        // clear global data
        clear();
    }

    static void fin() {
        // clear global data
        clear();
    }

    static void add_elem(std::size_t const tx_id, read_area_type const ra) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_cont()};
        get_cont()[tx_id] = ra;
    }

    static void remove_elem(std::size_t const tx_id) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_cont()};
        auto itr = get_cont().find(tx_id);
        if (itr != get_cont().end()) {
            // found
            get_cont().erase(tx_id);
        }
    }

    static read_area_type get_elem(std::size_t const tx_id) {
        std::shared_lock<std::shared_mutex> lk{get_mtx_cont()};
        auto itr = get_cont().find(tx_id);
        if (itr != get_cont().end()) {
            // found
            return itr->second;
        }
        LOG(ERROR) << log_location_prefix << "may be some error.";
        return {};
    }

    static bool
    check_potential_read_anti(std::size_t const tx_id,
                              std::set<Storage> const& write_storages);

    // getter / setter
    static cont_type& get_cont() { return cont_; }

    static std::shared_mutex& get_mtx_cont() { return mtx_cont_; }

private:
    /**
     * @brief mutex for container.
     */
    static inline std::shared_mutex mtx_cont_;

    /**
     * @brief container for read area
    */
    static inline cont_type cont_;
};

} // namespace shirakami