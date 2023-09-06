
#include "database/include/tx_state_notification.h"

#ifdef PWAL

#include "concurrency_control/include/lpwal.h"

#endif

namespace shirakami {

void add_durability_callbacks(durability_callback_type const dc) {
#ifdef PWAL
    // enable logging
    dc(lpwal::get_durable_epoch());
#endif
    get_durability_callbacks().emplace_back(dc);
}

void call_durability_callbacks(durability_marker_type dm) {
    for (auto& elem : get_durability_callbacks()) { elem(dm); }
}

void clear_durability_callbacks() { get_durability_callbacks().clear(); }

Status register_durability_callback(durability_callback_type const cb) {
    add_durability_callbacks(cb);
    return Status::OK;
}

} // namespace shirakami