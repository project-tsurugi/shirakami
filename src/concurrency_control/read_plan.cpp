
#include <algorithm>
#include <set>

#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

#include "glog/logging.h"

namespace shirakami {

bool read_plan::check_potential_read_anti(
        std::size_t const tx_id, std::set<Storage> const& write_storages, bool check_all_tx) {
    bool hit = false;
    std::shared_lock<std::shared_mutex> lk{get_mtx_cont()};
    for (auto elem : get_cont()) {
        if (elem.first > tx_id) {
            LOG(ERROR) << log_location_prefix
                       << "container is ordered by tx id and it scan from low "
                          "number. why it missed own and see high priori?";
            return hit;
        }
        if (elem.first == tx_id) { return hit; }
        // elem is high priori tx
        auto plist = elem.second.get_positive_list();
        auto nlist = elem.second.get_negative_list();

        // cond1 empty and empty
        if (plist.empty() && nlist.empty()) {
            // it may read all
            VLOG(log_debug_timing_event)
                    << "/:shirakami:wait_reason:read_area ltx_id:"
                    << tx_id << " waiting, reason: high priori tx "
                    << "(ltx_id:" << elem.first << ") may read all storage";
            if (check_all_tx) {
                hit = true;
                continue;
            }
            return true;
        }

        for (auto st : write_storages) {
            // cond3 only nlist
            if (plist.empty()) {
                auto itr = nlist.find(st);
                if (itr == nlist.end()) {
                    VLOG(log_debug_timing_event)
                            << "/:shirakami:wait_reason:read_area ltx_id:"
                            << tx_id << " waiting, reason: high priori tx "
                            << "(ltx_id:" << elem.first << ") may read "
                            << "storage " << st;
                    if (check_all_tx) {
                        hit = true;
                        break;
                    }
                    return true;
                }
            }

            // cond2,4 only plist or p and n
            if (nlist.empty() || (!plist.empty() && !nlist.empty())) {
                auto itr = plist.find(st);
                if (itr != plist.end()) {
                    VLOG(log_debug_timing_event)
                            << "/:shirakami:wait_reason:read_area ltx_id:"
                            << tx_id << " waiting, reason: high priori tx "
                            << "(ltx_id:" << elem.first << ") may read "
                            << "storage " << st;
                    if (check_all_tx) {
                        hit = true;
                        break;
                    }
                    return true;
                }
            }
        }
    }

    LOG(ERROR) << log_location_prefix
               << "container is ordered by tx id and it scan from low "
                  "number. why it missed own and see high priori?";
    return hit;
}

} // namespace shirakami