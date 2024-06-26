
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

void local_write_set::push(Token token, write_set_obj&& elem) {
    std::lock_guard<std::shared_mutex> lk{get_mtx()};

    if (get_for_batch()) {
        if (elem.get_op() == OP_TYPE::DELETE) {
            cont_for_bt_.insert_or_assign(elem.get_rec_ptr(),
                                          write_set_obj(elem.get_storage(),
                                                        elem.get_op(),
                                                        elem.get_rec_ptr()));
        } else {
            cont_for_bt_.insert_or_assign(
                    elem.get_rec_ptr(),
                    write_set_obj(elem.get_storage(), elem.get_op(),
                                  elem.get_rec_ptr(), elem.get_value_view(),
                                  elem.get_inc_tombstone()));
        }

        if (static_cast<session*>(token)->get_tx_type() ==
            transaction_options::transaction_type::LONG) {
            // update storage map
            auto& smap = get_storage_map();
            // find about storage
            auto itr = smap.find(elem.get_storage());
            std::string key{};
            elem.get_key(key);
            if (itr == smap.end()) {
                // not found
                smap.emplace(elem.get_storage(), std::make_tuple(key, key));
            } else {
                // found, check left key
                if (key < std::get<0>(itr->second)) {
                    std::get<0>(itr->second) = key;
                } // check right key
                if (std::get<1>(itr->second) < key) {
                    std::get<1>(itr->second) = key;
                }
            }
        }
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
    auto ret = set().emplace(id, std::make_tuple(version, value));
    if (!ret.second) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix
                              << "unreachable path. maybe mixed some access.";
        return Status::ERR_FATAL;
    }
    return Status::OK;
}

} // namespace shirakami
