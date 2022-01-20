#include "concurrency_control/wp/include/read_by.h"

namespace shirakami {

read_by::body_type read_by::get_and_gc(epoch::epoch_t const epoch,
                                       epoch::epoch_t const threshold) {
    std::unique_lock<std::mutex> lk(mtx_);
    std::vector<body_elem_type> ret;
    for (auto itr = body_.begin(); itr != body_.end();) {
        // check for return
        if ((*itr).first == epoch) { ret.emplace_back(*itr); }

        if ((*itr).first < threshold) {
            itr = body_.erase(itr);
        } else {
            ++itr;
        }
    }

    return ret;
}

void read_by::push(body_elem_type const elem) {
    std::unique_lock<std::mutex> lk(mtx_);
    body_.emplace_back(elem);
    for (auto itr = body_.begin(); itr != body_.end(); ++itr) {
        if ((*itr).first < elem.first) {
            continue;
        } else if ((*itr).first == elem.first) {
            if ((*itr).second > elem.second) { (*itr).second = elem.second; }
            return;
        } else {
            body_.insert(itr, elem);
            return;
        }
    }
}

} // namespace shirakami