
#include <algorithm>
#include <set>

#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

#include "glog/logging.h"

namespace shirakami {

bool read_plan::check_potential_read_anti(
        std::size_t const tx_id, std::set<Storage> const& write_storages) {
    std::shared_lock<std::shared_mutex> lk{get_mtx_cont()};
    for (auto elem : get_cont()) {
        if (elem.first > tx_id) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix
                       << "container is ordered by tx id and it scan from low "
                          "number. why it missed own and see high priori?";
            return false;
        }
        if (elem.first == tx_id) { return false; }
        // elem is high priori tx
        auto plist = std::get<0>(elem.second).get_positive_list();
        auto nlist = std::get<0>(elem.second).get_negative_list();

        // cond1 empty and empty
        if (plist.empty() && nlist.empty()) {
            // it may read all
            return true;
        }

        for (auto elem : write_storages) {
            // cond3 only nlist
            if (plist.empty()) {
                auto itr = nlist.find(elem);
                if (itr == nlist.end()) { return true; }
            }

            // cond2,4 only plist or p and n
            if (nlist.empty() || (!plist.empty() && !nlist.empty())) {
                auto itr = plist.find(elem);
                if (itr != plist.end()) { return true; }
            }
        }
    }

    LOG_FIRST_N(ERROR, 1) << log_location_prefix
               << "container is ordered by tx id and it scan from low "
                  "number. why it missed own and see high priori?";
    return false;
}

} // namespace shirakami