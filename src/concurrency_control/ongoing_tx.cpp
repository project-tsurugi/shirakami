
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/wp.h"

namespace shirakami {

Status ongoing_tx::change_epoch_without_lock(std::size_t const tx_id,
                                             epoch::epoch_t const new_ep) {
    for (auto&& elem : tx_info_) {
        if (std::get<ongoing_tx::index_id>(elem) == tx_id) {
            std::get<ongoing_tx::index_epoch>(elem) = new_ep;
            return Status::OK;
        }
    }
    return Status::WARN_NOT_FOUND;
}

bool ongoing_tx::exist_id(std::size_t id) {
    std::shared_lock<std::shared_mutex> lk{mtx_};
    for (auto&& elem : tx_info_) {
        if (std::get<ongoing_tx::index_id>(elem) == id) { return true; }
    }
    return false;
}

bool ongoing_tx::exist_wait_for(session* ti) {
    std::shared_lock<std::shared_mutex> lk{mtx_};
    std::size_t id = ti->get_long_tx_id();
    bool has_wp = !ti->get_wp_set().empty();
    auto wait_for = ti->extract_wait_for();
    // TODO wait_for empty return false.
    // check local write set
    std::set<Storage> st_set{};
    // create and compaction about storage set
    ti->get_write_set().get_storage_set(st_set);

    // check wait
    for (auto&& elem : tx_info_) {
        // check overwrites
        if (wait_for.find(std::get<ongoing_tx::index_id>(elem)) !=
            wait_for.end()) {
            // wait_for hit.
            if (std::get<ongoing_tx::index_id>(elem) < id) { return true; }
        }
        if (has_wp) {
            // check potential read-anti
            if (std::get<ongoing_tx::index_id>(elem) < id) {
                // check read area
                // check each storage
                for (auto st : st_set) {
                    wp::page_set_meta* out{};
                    auto rc = find_page_set_meta(st, out);
                    if (rc == Status::WARN_NOT_FOUND) {
                        break; // todo error handling
                    }
                    if (rc != Status::OK) {
                        LOG(ERROR) << log_location_prefix
                                   << "It strongly suspect that DML and DDL "
                                      "are mixed.";
                        break;
                    }
                    // check plist
                    auto plist = out->get_read_plan().get_positive_list();
                    for (auto p_id : plist) {
                        if (p_id == std::get<ongoing_tx::index_id>(elem)) {
                            return true;
                        }
                    }
                    //// check nlist // todo remove after impl compliment
                    //// between p and n.
                    //auto nlist = out->get_read_plan().get_negative_list();
                    //bool n_hit{false};
                    //for (auto n_id : nlist) {
                    //    if (n_id == std::get<ongoing_tx::index_id>(elem)) {
                    //        n_hit = true;
                    //        break;
                    //    }
                    //}
                    //if (n_hit) { continue; }
                    //// empty read positive mean universe.
                    //return true;
                }
            }
        }
    }
    return false;
}

void ongoing_tx::push(tx_info_elem_type const ti) {
    std::lock_guard<std::shared_mutex> lk{mtx_};
    if (tx_info_.empty()) {
        set_lowest_epoch(std::get<ongoing_tx::index_epoch>(ti));
    }
    tx_info_.emplace_back(ti);
}

void ongoing_tx::push_bringing_lock(tx_info_elem_type const ti) {
    if (tx_info_.empty()) {
        set_lowest_epoch(std::get<ongoing_tx::index_epoch>(ti));
    }
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
                if (std::get<ongoing_tx::index_epoch>(*it) < lep) {
                    lep = std::get<ongoing_tx::index_epoch>(*it);
                }
            }

            ++it;
        }
    }
    if (tx_info_.empty()) {
        set_lowest_epoch(0);
    } else {
        set_lowest_epoch(lep);
    }
    if (!erased) { LOG(ERROR) << log_location_prefix << "unreachable path."; }
}

} // namespace shirakami