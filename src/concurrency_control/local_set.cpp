
#include <algorithm>

#include "include/local_set.h"

#include "atomic_wrapper.h"

namespace shirakami {

Status local_write_set::erase(write_set_obj* wso) {
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

void local_write_set::push(write_set_obj&& elem) {
    if (for_batch_) {
        cont_for_bt_.insert_or_assign(elem.get_rec_ptr(), std::move(elem));
    } else {
        cont_for_occ_.emplace_back(std::move(elem));
    }
}

write_set_obj* local_write_set::search(Record const* const rec_ptr) {
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
    std::sort(cont_for_occ_.begin(), cont_for_occ_.end());
}

void local_write_set::unlock() {
    auto process = [](Record* rec_ptr) {
        // inserted record's lock will be released at remove_inserted_records_of_write_set_from_masstree function.
        tid_word expected{};
        tid_word desired{};
        expected = loadAcquire(rec_ptr->get_tidw_ref().obj_); // NOLINT
        desired = expected;
        desired.set_lock(false);
        storeRelease(rec_ptr->get_tidw_ref().obj_, desired.obj_); // NOLINT
    };

    if (get_for_batch()) {
        for (auto&& elem : get_ref_cont_for_bt()) {
            write_set_obj* we_ptr = &std::get<1>(elem);
            if (we_ptr->get_op() == OP_TYPE::INSERT) continue;
            process(we_ptr->get_rec_ptr());
        }
    } else {
        for (auto&& elem : get_ref_cont_for_occ()) {
            write_set_obj* we_ptr = &elem;
            if (we_ptr->get_op() == OP_TYPE::INSERT) continue;
            process(we_ptr->get_rec_ptr());
        }
    }
}

void local_write_set::unlock(std::size_t num) {
    auto process = [](write_set_obj* we_ptr) {
        tid_word expected{};
        tid_word desired{};
        expected = loadAcquire(
                we_ptr->get_rec_ptr()->get_tidw_ref().get_obj()); // NOLINT
        desired = expected;
        desired.set_lock(false);
        storeRelease(we_ptr->get_rec_ptr()->get_tidw_ref().get_obj(),
                     desired.get_obj()); // NOLINT
    };
    std::size_t ctr{0};
    if (get_for_batch()) {
        auto& cont = get_ref_cont_for_bt();
        for (auto&& elem : cont) {
            if (num == ctr) return;
            write_set_obj* we_ptr = &std::get<1>(elem);
            if (we_ptr->get_op() != OP_TYPE::INSERT) {
                // inserted record's lock will be released at remove_inserted_records_of_write_set_from_masstree function.
                process(we_ptr);
            }
            ++ctr;
        }
    } else {
        auto& cont = get_ref_cont_for_occ();
        for (auto&& elem : cont) {
            if (num == ctr) return;
            write_set_obj* we_ptr = &elem;
            if (we_ptr->get_op() != OP_TYPE::INSERT) {
                // inserted record's lock will be released at remove_inserted_records_of_write_set_from_masstree function.
                process(we_ptr);
            }
            ++ctr;
        }
    }
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
    auto ret =
            set().insert(std::make_pair(id, std::make_tuple(version, value)));
    if (!ret.second) {
        LOG(ERROR) << "unexpected code path";
        return Status::ERR_FATAL;
    }
    return Status::OK;
}

} // namespace shirakami