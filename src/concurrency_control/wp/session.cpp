
#include "include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "glog/logging.h"

namespace shirakami {

bool session::check_exist_wp_set(Storage storage) const {
    for (auto&& elem : get_wp_set()) {
        if (elem.first == storage) { return true; }
    }
    return false;
}

void session::clear_local_set() {
    node_set_.clear();
    point_read_by_bt_set_.clear();
    range_read_by_bt_set_.clear();
    read_by_short_set_.clear();
    read_set_.clear();
    wp_set_.clear();
    write_set_.clear();
    overtaken_ltx_set_.clear();
}

void session::clear_tx_property() { set_tx_began(false); }

Status session::find_high_priority_short() {
    if (get_tx_type() != TX_TYPE::LONG) {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    for (auto&& itr : session_table::get_session_table()) {
        if (itr.get_visible() && itr.get_tx_type() == TX_TYPE::SHORT &&
            !itr.get_read_only() && itr.get_operating() &&
            itr.get_step_epoch() < get_valid_epoch()) {
            return Status::WARN_PREMATURE;
        }
    }
    return Status::OK;
}

Status session::find_wp(Storage st) const {
    for (auto&& elem : get_wp_set()) {
        if (elem.first == st) { return Status::OK; }
    }
    return Status::WARN_NOT_FOUND;
}

} // namespace shirakami