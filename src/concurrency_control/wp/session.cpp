
#include "include/session.h"
#include "include/tuple_local.h"

namespace shirakami {

bool session::check_exist_wp_set(Storage storage) const {
    for (auto&& elem : get_wp_set()) {
        if (elem == storage) { return true; }
    }
    return false;
}

void session::clean_up_local_set() {
    node_set_.clear();
    read_by_set_.clear();
    read_set_.clear();
    wp_set_.clear();
    write_set_.clear();
    storage_set_.clear();
}

void session::clean_up_tx_property() { set_tx_began(false); }

Status session::find_wp(Storage st) const {
    for (auto&& elem : get_wp_set()) {
        if (elem == st) { return Status::OK; }
    }
    return Status::WARN_NOT_FOUND;
}

} // namespace shirakami