
#include "database/include/tx_state_notification.h"
#include "concurrency_control/include/epoch.h"
#include "database/include/logging.h"

#ifdef PWAL

#include "concurrency_control/include/lpwal.h"

#endif

namespace shirakami {

void add_durability_callbacks(durability_callback_type const& dc) {
    std::unique_lock<std::mutex> lk{get_mtx_durability_callbacks()};
#ifdef PWAL
    // enable logging
    dc(epoch::get_datastore_durable_epoch());
#endif
    get_durability_callbacks().emplace_back(dc);
}

void call_durability_callbacks(durability_marker_type dm) {
    std::unique_lock<std::mutex> lk{get_mtx_durability_callbacks()};
    for (auto& elem : get_durability_callbacks()) {
        if (elem) {
            // elem is callable
            elem(dm);
        }
    }
}

void clear_durability_callbacks() {
    std::unique_lock<std::mutex> lk{get_mtx_durability_callbacks()};
    get_durability_callbacks().clear();
}

Status
register_durability_callback(durability_callback_type const cb) { // NOLINT
    shirakami_log_entry << "register_durability_callback";
    add_durability_callbacks(cb);
    shirakami_log_exit << "register_durability_callback";
    return Status::OK;
}

} // namespace shirakami