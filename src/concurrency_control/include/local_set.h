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
    write_set_obj* find(Record* rec_ptr);

    template<class T>
    T&& get_cont();

    bool get_for_batch() { return for_batch_; }

    void push(write_set_obj&& elem);

    void set_for_batch(bool tf) { for_batch_ = tf; }

    void sort_if_ol();

private:
    /**
     * @brief A flag that identifies whether the container is for batch processing or online processing.
     */
    bool for_batch_{false};
    std::vector<write_set_obj> cont_for_ol_;
    std::map<Record*, write_set_obj> cont_for_bt_;
};

} // namespace shirakami