#include "concurrency_control/wp/include/read_by.h"
#include "concurrency_control/wp/include/ongoing_tx.h"

#include "glog/logging.h"

namespace shirakami {

read_by_bt::body_elem_type read_by_bt::get(epoch::epoch_t const epoch) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (auto itr = body_.begin(); itr != body_.end(); ++itr) {
        if ((*itr).first == epoch) {
            return *itr;
        } else if ((*itr).first > epoch) {
            // no more due to invariant
            break;
        }
    }

    return body_elem_type{0, 0};
}

void read_by_bt::gc() {
    const auto ce = epoch::get_global_epoch();
    auto threshold = ongoing_tx::get_lowest_epoch();
    if (threshold == 0) { threshold = ce; }
    for (auto itr = body_.begin(); itr != body_.end();) {
        if ((*itr).first < threshold) {
            itr = body_.erase(itr);
        } else {
            // no more gc
            break;
        }
    }
}

void read_by_bt::push(body_elem_type const elem) {
    std::unique_lock<std::mutex> lk(mtx_);
    const auto ce = epoch::get_global_epoch();
    auto threshold = ongoing_tx::get_lowest_epoch();
    if (threshold == 0) { threshold = ce; }
    for (auto itr = body_.begin(); itr != body_.end();) {
        if ((*itr).first < elem.first) {
            // check gc
            if ((*itr).first < threshold) {
                itr = body_.erase(itr);
            } else {
                ++itr;
            }
            continue;
        }
        if ((*itr).first == elem.first) {
            if ((*itr).second > elem.second) { (*itr).second = elem.second; }
            return;
        }
        body_.insert(itr, elem);
        return;
    }
}

void read_by_occ::gc() {
#if PARAM_READ_BY_MODE == 0
    const auto ce = epoch::get_global_epoch();
    auto threshold = ongoing_tx::get_lowest_epoch();
    if (threshold == 0) { threshold = ce; }
    for (auto itr = body_.begin(); itr != body_.end();) {
        if ((*itr) < threshold) {
            itr = body_.erase(itr);
        } else {
            return;
        }
    }
#endif
}

bool read_by_occ::find(epoch::epoch_t const epoch) {
#if PARAM_READ_BY_MODE == 0
    std::unique_lock<std::mutex> lk(mtx_);
    const auto ce = epoch::get_global_epoch();
    auto threshold = ongoing_tx::get_lowest_epoch();
    if (threshold == 0) { threshold = ce; }
    for (auto itr = body_.begin(); itr != body_.end();) {
        if ((*itr) < epoch) {
            // check gc
            if ((*itr) < threshold) {
                itr = body_.erase(itr);
            } else {
                ++itr;
            }
            continue;
        }
        if ((*itr) == epoch) { // found
            return *itr;
        }
        return false;
    }

    return false;
#elif PARAM_READ_BY_MODE == 1
    return get_max_epoch() == epoch;
#endif
}

void read_by_occ::push(body_elem_type const elem) {
#if PARAM_READ_BY_MODE == 0
    // optimization
    if (get_max_epoch() == elem) { return; }

    std::unique_lock<std::mutex> lk(mtx_);

    // if empty
    if (body_.empty()) {
        // push back
        body_.emplace_back(elem);
        set_max_epoch(elem);
        return;
    }

    for (auto ritr = body_.rbegin(); ritr != body_.rend(); ++ritr) {
        if ((*ritr) < elem) {
            if (ritr == body_.rbegin()) { set_max_epoch(elem); }
            body_.insert(ritr.base(), elem);
            gc();
            return;
        }
    }

    body_.insert(body_.begin(), elem);

    gc();
    return;
#elif PARAM_READ_BY_MODE == 1

    auto& me = get_max_epoch_ref();
    auto ce = get_max_epoch();
    for (;;) {
        if (ce < elem) {
            if (me.compare_exchange_weak(ce, elem, std::memory_order_release,
                                         std::memory_order_acquire)) {
                break;
            }
        } else {
            break;
        }
    }
    return;

#endif
}

} // namespace shirakami