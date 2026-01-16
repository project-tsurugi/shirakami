
#include <algorithm>

#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "database/include/logging.h"

namespace shirakami {

bool ongoing_tx::exist_id(std::size_t id) {
    std::shared_lock<std::shared_mutex> lk{mtx_};
    for (auto&& elem : tx_info_) {
        if (std::get<ongoing_tx::index_id>(elem) == id) { return true; }
    }
    return false;
}

void ongoing_tx::push(tx_info_elem_type const ti) {
    std::lock_guard<std::shared_mutex> lk{mtx_};
    tx_info_.emplace_back(ti);
}

void ongoing_tx::push_bringing_lock(tx_info_elem_type const ti) {
    tx_info_.emplace_back(ti);
}

void ongoing_tx::remove_id(std::size_t const id) {
    std::lock_guard<std::shared_mutex> lk{mtx_};
    epoch::epoch_t lep{0};
    bool first{true};
    bool erased{false};
    for (auto it = tx_info_.begin(); it != tx_info_.end();) { // NOLINT
        if (!erased && std::get<ongoing_tx::index_id>(*it) == id) {
            tx_info_.erase(it);
            // TODO: it = ?
            erased = true;
        } else {
            // update lowest epoch
            if (first) {
                lep = std::get<ongoing_tx::index_epoch>(*it);
                first = false;
            } else {
                lep = std::min(lep, std::get<ongoing_tx::index_epoch>(*it));
            }

            ++it;
        }
    }
    if (!erased) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path.";
    }
}

void ongoing_tx::set_optflags() {
    // check environ "SHIRAKAMI_ENABLE_WAITING_BYPASS"
    constexpr bool enable_wb_default = false;
    bool enable_wb = enable_wb_default;
    if (auto* envstr = std::getenv("SHIRAKAMI_ENABLE_WAITING_BYPASS");
        envstr != nullptr && *envstr != '\0') {
        if (std::strcmp(envstr, "1") == 0) {
            enable_wb = true;
        } else if (std::strcmp(envstr, "0") == 0) {
            enable_wb = false;
        } else {
            VLOG(log_debug)
                    << log_location_prefix << "invalid value is set for "
                    << "SHIRAKAMI_ENABLE_WAITING_BYPASS; using default value";
        }
    }
    VLOG(log_debug) << log_location_prefix << "optflag: waiting bypass is "
                    << (enable_wb ? "enabled" : "disabled")
                    << (enable_wb == enable_wb_default ? " (default)" : "");
    optflag_disable_waiting_bypass_ = !enable_wb;
    // check environ "SHIRAKAMI_WAITING_BYPASS_TO_ROOT"
    bool is_envset = false;
    if (auto* envstr = std::getenv("SHIRAKAMI_WAITING_BYPASS_TO_ROOT");
        envstr != nullptr && *envstr != '\0') {
        is_envset = (std::strcmp(envstr, "1") == 0);
    }
    optflag_waiting_bypass_to_root_ = is_envset;
    VLOG(log_debug) << log_location_prefix << "optflag: bypass to root "
                    << (is_envset ? "on" : "off");
}

} // namespace shirakami
