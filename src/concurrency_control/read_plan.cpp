
#include <algorithm>
#include <set>

#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "glog/logging.h"

namespace shirakami {

// check overlap between { w_lkey:INCLUSIVE, w_rkey:INCLUSIVE } and { r_lkey:r_lpoint, r_rkey:r_rpoint }
// TODO: use some range operation library
bool read_plan::check_range_overlap(
        const std::string& w_lkey, const std::string& w_rkey,
        const std::string& r_lkey, scan_endpoint r_lpoint, const std::string& r_rkey, scan_endpoint r_rpoint) {
    // define write range [], read range ()
    bool no_overlap = (
            // case: []()
               (r_lpoint == scan_endpoint::INCLUSIVE && w_rkey < r_lkey)
            || (r_lpoint == scan_endpoint::EXCLUSIVE && w_rkey <= r_lkey)
            // case: ()[]
            || (r_rpoint == scan_endpoint::INCLUSIVE && w_lkey > r_rkey)
            || (r_rpoint == scan_endpoint::EXCLUSIVE && w_lkey >= r_rkey)
    );
    return !no_overlap;
}

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

        for (auto&& st :
             static_cast<session*>(token)->get_write_set().get_storage_map()) {
            // cond3 only nlist
            if (plist.empty()) {
                // the higher priori ltx is not submitted commit
                auto itr = nlist.find(st.first);
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
                        if (std::get<0>(p_elem) == st.first) {
                            // hit
                            return true;
                        }
                    } else {
                        // it submit commit
                        // check conflict storage level
                        if (std::get<0>(p_elem) == st.first) {
                            // check key range level
                            // todo: use constant value, not magic number
                            bool hit = check_range_overlap(
                                    std::get<0>(st.second), std::get<1>(st.second),
                                    std::get<2>(p_elem), std::get<3>(p_elem),
                                    std::get<4>(p_elem), std::get<5>(p_elem)); // NOLINT
                            if (hit) { return true; }
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
