#include "concurrency_control/include/read_by.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"

#include "glog/logging.h"

namespace shirakami {

bool point_read_by_long::is_exist(Token token) {
    auto* ti = static_cast<session*>(token);
    const epoch::epoch_t epoch = ti->get_valid_epoch();
    const std::size_t ltx_id = ti->get_long_tx_id();
    std::shared_lock<std::shared_mutex> lk(mtx_);
    for (auto&& elem : body_) {
        if (elem.second < ltx_id) {
            // elem is high priori than this.
            if (epoch <= elem.first) {
                /**
                 * reason to include =. The order of ltxs which has same epoch
                 *  is undefined.
                 */
                return true;
            }
        } else if (elem.second == ltx_id) {
            LOG(ERROR) << "programming error";
            return true;
        } else {
            // elem is low priori than this.
            break;
        }
    }

    return false;
}

void point_read_by_long::push(body_elem_type const elem) {
    std::lock_guard<std::shared_mutex> lk(mtx_);
    const auto ce = epoch::get_global_epoch();
    auto threshold = ongoing_tx::get_lowest_epoch();
    if (threshold == 0) { threshold = ce; }
    for (auto itr = body_.begin(); itr != body_.end();) { // NOLINT
        if ((*itr).second < elem.second) {
            // high priori
            // check gc
            if ((*itr).first < threshold) {
                itr = body_.erase(itr);
            } else {
                break;
            }
            continue;
        }
        if ((*itr).second == elem.second) {
            LOG(ERROR) << "programming error";
            return;
        }
        // low priori
        break;
    }
    body_.emplace_back(elem);
}

void point_read_by_long::print() {
    LOG(INFO) << ">> print point_read_by_long";
    for (auto itr = body_.begin(); itr != body_.end();) { // NOLINT
        LOG(INFO) << (*itr).first << ", " << (*itr).second;
    }
    LOG(INFO) << "<< print point_read_by_long";
}

bool range_read_by_long::is_exist(epoch::epoch_t const ep,
                                  std::string_view const key) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (auto&& elem : body_) {
        if (std::get<range_read_by_long::index_epoch>(elem) == ep) {
            // check the key is right from left point
            if (std::get<range_read_by_long::index_l_ep>(elem) ==
                        scan_endpoint::INF || // inf
                std::get<range_read_by_long::index_l_key>(elem) <
                        key || // right
                (std::get<range_read_by_long::index_l_key>(elem) == key &&
                 std::get<range_read_by_long::index_l_ep>(elem) ==
                         scan_endpoint::INCLUSIVE) // same
            ) {
                // check the key is left from right point
                if (std::get<range_read_by_long::index_r_ep>(elem) ==
                            scan_endpoint::INF || // inf
                    std::get<range_read_by_long::index_r_key>(elem) >
                            key || // left
                    (std::get<range_read_by_long::index_r_key>(elem) == key &&
                     std::get<range_read_by_long::index_r_ep>(elem) ==
                             scan_endpoint::INCLUSIVE) // same
                ) {
                    return true;
                }
            }
        }
        if (std::get<range_read_by_long::index_epoch>(elem) > ep) {
            // no more due to invariant
            break;
        }
    }

    return false;
}

void range_read_by_long::push(body_elem_type const& elem) {
    std::unique_lock<std::mutex> lk(mtx_);
    const auto ce = epoch::get_global_epoch();
    auto threshold = ongoing_tx::get_lowest_epoch();
    if (threshold == 0) { threshold = ce; }
    std::size_t tx_id = std::get<range_read_by_long::index_tx_id>(elem);
    for (auto itr = body_.begin(); itr != body_.end();) { // NOLINT
        if (std::get<range_read_by_long::index_tx_id>(*itr) < tx_id) {
            // high priori
            if (std::get<range_read_by_long::index_epoch>(*itr) < threshold) {
                // gc
                itr = body_.erase(itr);
                continue;
            }
            // can't gc
            break;
        }
        if (std::get<range_read_by_long::index_tx_id>(*itr) == tx_id) {
            LOG(ERROR) << "programming error";
            return;
        }
        // low priori
        break;
    }
    body_.emplace_back(elem);
}

bool point_read_by_short::find(epoch::epoch_t const epoch) {
    return get_max_epoch() == epoch;
}

void point_read_by_short::push(epoch::epoch_t const elem) {
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
}

bool range_read_by_short::find(epoch::epoch_t const epoch) {
    return get_max_epoch() == epoch;
}

void range_read_by_short::push(epoch::epoch_t const elem) {
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
}

} // namespace shirakami