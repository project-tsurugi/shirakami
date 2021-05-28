/**
 * @file src/concurrency_control/include/local_set.h
 */

#pragma once

#include <map>
#include <utility>

#include "concurrency_control/include/local_set_scheme.h"
#include "concurrency_control/include/record.h"

namespace shirakami {

/**
 * @brief For local write set
 */
class local_write_set {
public:
    /**
     * @param[in] rec_ptr The key to be found.
     * @return some_ptr Found element.
     * @return nullptr Not found.
     */
    write_set_obj* find(Record* rec_ptr) {
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

    bool get_for_batch() { return for_batch_; }

    void push(write_set_obj&& elem) {
        if (for_batch_) {
            cont_for_bt_.insert_or_assign(elem.get_rec_ptr(), std::move(elem));
        } else {
            cont_for_ol_.emplace_back(std::move(elem));
        }
    }

    void set_for_batch(bool tf) { for_batch_ = tf; }

    void sort_if_ol() {
        if (for_batch_) return;
        std::sort(cont_for_ol_.begin(), cont_for_ol_.end());
    }

private:
    /**
     * @brief A flag that identifies whether the container is for batch processing or online processing.
     */
    bool for_batch_{false};
    std::vector<write_set_obj> cont_for_ol_;
    std::map<Record*, write_set_obj> cont_for_bt_;
};

} // namespace shirakami