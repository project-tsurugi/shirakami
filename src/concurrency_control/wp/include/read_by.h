/**
 * @file read_by.h
 * @author your name (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2022-01-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#pragma once

#include <mutex>
#include <vector>

#include "concurrency_control/wp/include/epoch.h"

namespace shirakami {

class read_by {
public:
    using body_elem_type = std::pair<epoch::epoch_t, std::size_t>;
    using body_type = std::vector<body_elem_type>;

    /**
     * @brief Get the partial elements and gc stale elements
     * @param epoch 
     * @param threshold In the process of searching, remove the element with 
     * epoch smaller than threshold.
     * @return std::vector<body_elem_type> 
     */
    body_elem_type get_and_gc(epoch::epoch_t epoch, epoch::epoch_t threshold);

    void push(body_elem_type elem);

private:
    std::mutex mtx_;

    /**
     * @brief body
     * @details std::pair.first is epoch. the second is batch_id.
     */
    body_type body_;
};

} // namespace shirakami