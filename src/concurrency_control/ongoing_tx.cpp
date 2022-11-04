
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/wp.h"

namespace shirakami {

Status ongoing_tx::change_epoch_without_lock(std::size_t const tx_id,
                                             epoch::epoch_t const new_ep) {
    for (auto&& elem : tx_info_) {
        if (elem.second == tx_id) {
            elem.first = new_ep;
            return Status::OK;
        }
    }
    return Status::WARN_NOT_FOUND;
}

Status ongoing_tx::change_epoch_without_lock(
        std::size_t const id, epoch::epoch_t const ep,
        std::size_t const need_id, epoch::epoch_t const need_id_epoch) {
    bool exist_id{false};
    bool exist_need_id{false};
    tx_info_elem_type* target{};
    for (auto&& elem : tx_info_) {
        if (!exist_id && elem.second == id) {
            exist_id = true;
            target = &elem;
        }
        if (!exist_need_id && elem.second == need_id) {
            if (elem.first == need_id_epoch) {
                exist_need_id = true;
            } else {
                // fail optimistic change due to concurrent forwarding.
                return Status::WARN_NOT_FOUND;
            }
        }
        if (exist_id && exist_need_id) {
            target->first = ep;
            return Status::OK;
        }
    }
    if (exist_id && !exist_need_id) { return Status::WARN_NOT_FOUND; }
    LOG(ERROR) << "programming error";
    return Status::ERR_FATAL;
}

bool ongoing_tx::exist_id(std::size_t id) {
    std::shared_lock<std::shared_mutex> lk{mtx_};
    for (auto&& elem : tx_info_) {
        if (elem.second == id) { return true; }
    }
    return false;
}

bool ongoing_tx::exist_wait_for(session* ti) {
    std::shared_lock<std::shared_mutex> lk{mtx_};
    std::size_t id = ti->get_long_tx_id();
    bool has_wp = !ti->get_wp_set().empty();
    auto wait_for = ti->extract_wait_for();
    // check local write set
    std::set<Storage> st_set{};
    // create and compaction about storage set
    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        st_set.insert(wso.second.get_storage());
    }
    // check wait
    for (auto&& elem : tx_info_) {
        // check overwrites
        if (wait_for.find(elem.second) != wait_for.end()) {
            // wait_for hit.
            if (elem.second < id) { return true; }
        }
        if (has_wp) {
            // check potential read-anti
            if (elem.second < id) {
                // check read area
                // check each storage
                for (auto st : st_set) {
                    wp::page_set_meta* out{};
                    auto rc = find_page_set_meta(st, out);
                    if (rc == Status::WARN_NOT_FOUND) {
                        break; // todo error handling
                    }
                    if (rc != Status::OK) {
                        LOG(ERROR) << "programming error";
                        break;
                    }
                    // check plist
                    auto plist = out->get_read_plan().get_positive_list();
                    for (auto p_id : plist) {
                        if (p_id == elem.second) { return true; }
                    }
                    // check nlist // todo remove after impl compliment
                    // between p and n.
                    auto nlist = out->get_read_plan().get_negative_list();
                    bool n_hit{false};
                    for (auto n_id : nlist) {
                        if (n_id == elem.second) {
                            n_hit = true;
                            break;
                        }
                    }
                    if (n_hit) { continue; }
                    // empty read positive mean universe.
                    return true;
                }
            }
        }
    }
    return false;
}

void ongoing_tx::push(tx_info_elem_type ti) {
    std::lock_guard<std::shared_mutex> lk{mtx_};
    if (tx_info_.empty()) { set_lowest_epoch(ti.first); }
    tx_info_.emplace_back(ti);
}

void ongoing_tx::push_bringing_lock(tx_info_elem_type ti) {
    if (tx_info_.empty()) { set_lowest_epoch(ti.first); }
    tx_info_.emplace_back(ti);
}

void ongoing_tx::remove_id(std::size_t id) {
    std::lock_guard<std::shared_mutex> lk{mtx_};
    epoch::epoch_t lep{0};
    bool first{true};
    bool erased{false};
    for (auto it = tx_info_.begin(); it != tx_info_.end();) {
        if (!erased && (*it).second == id) {
            tx_info_.erase(it);
            erased = true;
        } else {
            // update lowest epoch
            if (first) {
                lep = (*it).first;
                first = false;
            } else {
                if ((*it).first < lep) { lep = (*it).first; }
            }

            ++it;
        }
    }
    if (tx_info_.empty()) {
        set_lowest_epoch(0);
    } else {
        set_lowest_epoch(lep);
    }
    if (!erased) { LOG(ERROR) << "programming error."; }
}

} // namespace shirakami