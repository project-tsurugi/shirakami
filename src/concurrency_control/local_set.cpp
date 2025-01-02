
#include <algorithm>
#include <utility>

#include "concurrency_control/include/session.h"

#include "include/local_set.h"

#include "atomic_wrapper.h"

namespace shirakami {

Status local_write_set::erase(write_set_obj* wso) {
    std::lock_guard<std::shared_mutex> lk{get_mtx()};

    if (for_batch_) {
        auto result = get_ref_cont_for_bt().find(wso->get_rec_ptr());
        if (result == get_ref_cont_for_bt().end()) {
            return Status::WARN_NOT_FOUND;
        }
        get_ref_cont_for_bt().erase(result);
    } else {
        auto result = std::find(get_ref_cont_for_occ().begin(),
                                get_ref_cont_for_occ().end(), *wso);
        if (result == get_ref_cont_for_occ().end()) {
            return Status::WARN_NOT_FOUND;
        }
        get_ref_cont_for_occ().erase(result);
    }
    return Status::OK;
}

void local_write_set::push(Token, write_set_obj&& elem) {
    std::lock_guard<std::shared_mutex> lk{get_mtx()};

    if (get_for_batch()) {
        auto* rec_ptr = elem.get_rec_ptr();
        cont_for_bt_.insert_or_assign(rec_ptr, std::move(elem));
    } else {
        cont_for_occ_.emplace_back(std::move(elem)); // NOLINT
        if (cont_for_occ_.size() > 100) {            // NOLINT
            // swtich to use cont_for_bt_ for performance
            set_for_batch(true);
            for (auto&& elem_occ : cont_for_occ_) {
                cont_for_bt_.insert_or_assign(elem_occ.get_rec_ptr(), // NOLINT
                                              std::move(elem_occ));
            }
            // clear occ set
            cont_for_occ_.clear();
        }
    }
}

write_set_obj* local_write_set::search(Record const* const rec_ptr) {
    std::shared_lock<std::shared_mutex> lk{get_mtx()};

    if (for_batch_) {
        auto ret{cont_for_bt_.find(const_cast<Record*>(rec_ptr))};
        if (ret == cont_for_bt_.end()) { return nullptr; }
        return &std::get<1>(*ret);
    }
    for (auto&& elem : cont_for_occ_) {
        write_set_obj* we_ptr = &elem;
        if (rec_ptr == we_ptr->get_rec_ptr()) { return we_ptr; }
    }
    return nullptr;
}

void local_write_set::sort_if_ol() {
    if (for_batch_) return;
    std::lock_guard<std::shared_mutex> lk{get_mtx()};
    std::sort(cont_for_occ_.begin(), cont_for_occ_.end());
}

Status local_sequence_set::push(SequenceId const id,
                                SequenceVersion const version,
                                SequenceValue const value) {
    auto itr = set().find(id);
    if (itr != set().end()) {
        // found
        if (std::get<0>(itr->second) < version) {
            // new value is larger than old ones
            itr->second = std::make_tuple(version, value);
            return Status::OK;
        } // new value is smaller than old ones
        return Status::WARN_ALREADY_EXISTS;
    } // not found
    auto ret = set().emplace(id, std::make_tuple(version, value));
    if (!ret.second) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix
                              << "unreachable path. maybe mixed some access.";
        return Status::ERR_FATAL;
    }
    return Status::OK;
}

} // namespace shirakami
