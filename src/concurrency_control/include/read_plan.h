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
    /**
     * @details std::tuple: storage area, whether commit was requested, left key,
     * left endpoint, right key, right endpoint.
    */
    using plist_type =
            std::set<std::tuple<Storage, bool, std::string, scan_endpoint,
                                std::string, scan_endpoint>>;
    using nlist_type = std::set<Storage>;

    using cont_type = std::map<std::size_t, std::tuple<plist_type, nlist_type>>;

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

    // for tx begin
    static void add_elem(std::size_t const tx_id, read_area_type const& ra) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_cont()};
        plist_type tmp_plist;
        for (auto&& elem : ra.get_positive_list()) {
            tmp_plist.insert(std::make_tuple(elem, false, "",
                                             scan_endpoint::EXCLUSIVE, "",
                                             scan_endpoint::EXCLUSIVE));
        }
        get_cont()[tx_id] = std::make_tuple(tmp_plist, ra.get_negative_list());
    }

    // for commit submit
    static void add_elem(std::size_t const tx_id, plist_type const& pl,
                         nlist_type const& nl) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_cont()};
        get_cont()[tx_id] = std::make_tuple(pl, nl);
    }

    static void remove_elem(std::size_t const tx_id) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_cont()};
        auto itr = get_cont().find(tx_id);
        if (itr != get_cont().end()) {
            // found
            get_cont().erase(tx_id);
        }
    }

    static bool check_potential_read_anti(std::size_t tx_id, Token token);

    // getter / setter
    static cont_type& get_cont() { return cont_; }

    static std::shared_mutex& get_mtx_cont() { return mtx_cont_; }

private:
    /**
     * @brief mutex for container.
     */
    static inline std::shared_mutex mtx_cont_; // NOLINT

    /**
     * @brief container for read area
    */
    static inline cont_type cont_; // NOLINT
};

} // namespace shirakami
