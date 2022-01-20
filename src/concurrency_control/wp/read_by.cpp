#include "concurrency_control/wp/include/read_by.h"

namespace shirakami {

read_by_bt::body_elem_type
read_by_bt::get_and_gc(epoch::epoch_t const epoch,
                       epoch::epoch_t const threshold) {
    std::unique_lock<std::mutex> lk(mtx_);
    std::vector<body_elem_type> ret;
    for (auto itr = body_.begin(); itr != body_.end();) {
        if ((*itr).first < threshold) {
            itr = body_.erase(itr);
        } else {
            if ((*itr).first == epoch) { return *itr; }
            ++itr;
        }
    }

    return body_elem_type{0, 0};
}

void read_by_bt::push(body_elem_type const elem) {
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

read_by_occ::body_elem_type
read_by_occ::get_and_gc(epoch::epoch_t const epoch,
                        epoch::epoch_t const threshold) {
    std::unique_lock<std::mutex> lk(mtx_);
    std::vector<body_elem_type> ret;
    for (auto itr = body_.begin(); itr != body_.end();) {
        if ((*itr) < threshold) {
            itr = body_.erase(itr);
        } else {
            if ((*itr) == epoch) { return *itr; }
            ++itr;
        }
    }

    return body_elem_type{0};
}

void read_by_occ::push(body_elem_type const elem) {
    std::unique_lock<std::mutex> lk(mtx_);
    body_.emplace_back(elem);
    for (auto itr = body_.begin(); itr != body_.end(); ++itr) {
        if ((*itr) < elem) {
            continue;
        } else if ((*itr) == elem) {
            return;
        } else {
            body_.insert(itr, elem);
            return;
        }
    }
}

} // namespace shirakami