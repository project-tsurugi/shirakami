
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
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return true;
        } else {
            // elem is low priori than this.
            break;
        }
    }

    return false;
}

void point_read_by_long::push(body_elem_type const elem) {
    // lock
    std::lock_guard<std::shared_mutex> lk(mtx_);

    // prepare
    const auto ce = epoch::get_global_epoch();
    auto threshold = epoch::get_cc_safe_ss_epoch();
    if (threshold == 0) { threshold = ce; }

    // gc
    // TODO at 559: too erase, threshold must be cc safe ss epoch
    std::size_t erase_count{0};
    for (auto itr = body_.begin(); itr != body_.end();) { // LINT
        if ((*itr).second < elem.second) {
            // high priori
            // check gc
            if ((*itr).first < threshold) {
                ++erase_count;
                ++itr;
            } else {
                break;
            }
            continue;
        }
        if ((*itr).second == elem.second) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return;
        }
        // low priori
        break;
    }
    // erase in bulk
    if (erase_count > 0) {
        body_.erase(body_.begin(), body_.begin() + erase_count); // LINT
    }

    // push info
    body_.emplace_back(elem);
}

void point_read_by_long::print() {
    LOG(INFO) << ">> print point_read_by_long";
    for (auto itr = body_.begin(); itr != body_.end();) { // LINT
        LOG(INFO) << (*itr).first << ", " << (*itr).second;
    }
    LOG(INFO) << "<< print point_read_by_long";
}

bool range_read_by_long::is_exist(epoch::epoch_t const ep,
                                  std::size_t const ltx_id,
                                  std::string_view const key) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (auto&& elem : body_) {
        if (ltx_id > std::get<range_read_by_long::index_tx_id>(elem)) {
            // check against higher priori ltxs
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
                        (std::get<range_read_by_long::index_r_key>(elem) ==
                                 key &&
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
    }

    return false;
}

void range_read_by_long::push(body_elem_type const& elem) {
    // lock
    std::unique_lock<std::mutex> lk(mtx_);

    // prepare
    const auto ce = epoch::get_global_epoch();
    auto gc_threshold = epoch::get_cc_safe_ss_epoch();
    if (gc_threshold == 0) { gc_threshold = ce; }
    std::size_t tx_id = std::get<range_read_by_long::index_tx_id>(elem);

    // gc
    std::size_t erase_count{0};
    for (auto itr = body_.begin(); itr != body_.end();) { // LINT
        if (std::get<range_read_by_long::index_tx_id>(*itr) < tx_id) {
            // high priori
            if (std::get<range_read_by_long::index_epoch>(*itr) <
                gc_threshold) {
                // gc
                ++itr;
                ++erase_count;
                continue;
            }
            // can't gc
            break;
        }
        /**
         * Now, there is a case the ltx can commit ahead of high priori ltx.
         */
        break;
    }

    // erase in bulk
    if (erase_count > 0) {
        body_.erase(body_.begin(), body_.begin() + erase_count); // LINT
    }

    // push info
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
