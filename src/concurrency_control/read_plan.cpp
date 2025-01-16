
#include <algorithm>
#include <set>

#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "glog/logging.h"

namespace shirakami {

bool read_plan::check_potential_read_anti(std::size_t const tx_id,
                                          Token token) {
    std::shared_lock<std::shared_mutex> lk{get_mtx_cont()};
    for (auto elem : get_cont()) {
        if (elem.first > tx_id) {
            LOG_FIRST_N(ERROR, 1)
                    << log_location_prefix
                    << "container is ordered by tx id and it scan from low "
                       "number. why it missed own and see high priori?";
            return false;
        }
        if (elem.first == tx_id) { return false; }
        // elem is high priori tx
        auto plist = std::get<0>(elem.second);
        auto nlist = std::get<1>(elem.second);

        // cond1 empty and empty
        if (plist.empty() && nlist.empty()) {
            // it may read all
            return true;
        }

        for (auto&& elem :
             static_cast<session*>(token)->get_write_set().get_storage_map()) {
            // cond3 only nlist
            if (plist.empty()) {
                // the higher priori ltx is not submitted commit
                auto itr = nlist.find(elem.first);
                if (itr == nlist.end()) {
                    // the high priori ltx may read this
                    return true;
                }
            }

            // cond2,4 only plist or both: check write and plist conlifct
            if (nlist.empty() || (!plist.empty() && !nlist.empty())) {
                // find plist
                for (auto&& p_elem : plist) {
                    // if the high priori ltx didn't submit commit, check
                    // storage level
                    if (!std::get<1>(p_elem)) {
                        // it didn't submit commit
                        if (std::get<0>(p_elem) == elem.first) {
                            // hit
                            return true;
                        }
                    } else {
                        // it submit commit
                        // check conflict storage level
                        if (std::get<0>(p_elem) == elem.first) {
                            // check key range level
                            // todo: use constant value, not magic number
                            std::string w_lkey =
                                    std::get<0>(elem.second); // LINT
                            std::string w_rkey =
                                    std::get<1>(elem.second);         // LINT
                            std::string r_lkey = std::get<2>(p_elem); // LINT
                            scan_endpoint r_lpoint =
                                    std::get<3>(p_elem);              // LINT
                            std::string r_rkey = std::get<4>(p_elem); // LINT
                            scan_endpoint r_rpoint =
                                    std::get<5>(p_elem); // LINT
                            // define write range [], read range ()
                            if (
                                    // case: [(])
                                    ((w_lkey < r_lkey &&
                                      r_lpoint != scan_endpoint::INF) &&
                                     (w_rkey < r_rkey ||
                                      r_rpoint == scan_endpoint::INF))
                                    // case: ([])
                                    || ((r_lkey < w_lkey ||
                                         r_lpoint == scan_endpoint::INF) &&
                                        (w_lkey < r_rkey ||
                                         r_rpoint == scan_endpoint::INF))
                                    // case: ([)]
                                    || ((r_lkey < w_lkey ||
                                         r_lpoint == scan_endpoint::INF) &&
                                        (w_rkey < r_rkey &&
                                         r_rpoint != scan_endpoint::INF))) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }

    LOG_FIRST_N(ERROR, 1)
            << log_location_prefix
            << "container is ordered by tx id and it scan from low "
               "number. why it missed own and see high priori?";
    return false;
}

} // namespace shirakami
