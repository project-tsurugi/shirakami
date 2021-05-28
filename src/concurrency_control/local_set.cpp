/**
 * @file local_set.cpp
 */

#include "concurrency_control/include/local_set.h"

namespace shirakami {

write_set_obj* local_write_set::find(Record* rec_ptr) {
    // for bt
    if (for_batch_) {
        auto ret{cont_for_bt_.find(rec_ptr)};
        if (ret == cont_for_bt_.end()) {
            return nullptr;
        }
        return &std::get<1>(*ret);
    }
    // for ol
    for (auto&& elem : cont_for_ol_) {
        if (elem.get_rec_ptr() == rec_ptr) {
            return &elem;
        }
    }
    return nullptr;
}

template<class T>
T&& local_write_set::get_cont() {
    if (for_batch_) return cont_for_bt_;
    return cont_for_ol_;
}

void local_write_set::push(write_set_obj&& elem) {
    if (for_batch_) {
        cont_for_bt_.insert_or_assign(elem.get_rec_ptr(), std::move(elem));
    } else {
        cont_for_ol_.emplace_back(std::move(elem));
    }
}

void local_write_set::sort_if_ol() {
    if (for_batch_) return;
    std::sort(cont_for_ol_.begin(), cont_for_ol_.end());
}

} // namespace shirakami