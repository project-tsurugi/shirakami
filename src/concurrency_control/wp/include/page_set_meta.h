/**
 * @file concurrency_control/wp/include/page_set_meta.h
 */

#pragma once

#include "wp.h"

namespace shirakami {

class read_by {
public:
    using body_elem_type = std::pair<std::size_t, std::size_t>;

    /**
     * @brief Get the partial elements and gc stale elements
     * @param epoch 
     * @param threshold In the process of searching, remove the element with 
     * epoch smaller than threshold.
     * @return std::vector<body_elem_type> 
     */
    std::vector<body_elem_type> get_and_gc(std::size_t epoch,
                                           std::size_t threshold) {
        std::unique_lock<std::mutex> lk(mtx_);
        std::vector<body_elem_type> ret;
        for (auto itr = body_.begin(); itr != body_.end();) {
            // check for return
            if ((*itr).first == epoch) { ret.emplace_back(*itr); }

            if ((*itr).first < threshold) {
                itr = body_.erase(itr);
            } else {
                ++itr;
            }
        }

        return ret;
    }

    void push(body_elem_type elem) {
        std::unique_lock<std::mutex> lk(mtx_);
        body_.emplace_back(elem);
    }

private:
    std::mutex mtx_;

    /**
     * @brief body
     * @details std::pair.first is epoch. the second is batch_id.
     */
    std::vector<body_elem_type> body_;
};

class page_set_meta {
public:
    wp::wp_meta* get_wp_meta_ptr() { return &wp_meta_; }

private:
    read_by read_by_;
    wp::wp_meta wp_meta_;
};

} // namespace shirakami