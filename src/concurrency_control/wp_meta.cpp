
#include "concurrency_control/include/wp_meta.h"

#include "shirakami/logging.h"

#include "glog/logging.h"

namespace shirakami::wp {

bool wp_meta::empty(const wp_meta::wped_type& wped) {
    for (auto&& elem : wped) {
        if (elem != std::pair<epoch::epoch_t, std::size_t>(0, 0)) {
            return false;
        }
    }
    return true;
}

Status wp_meta::change_wp_epoch(std::size_t id, epoch::epoch_t target) {
    wp_lock_.lock();
    for (std::size_t i = 0; i < KVS_MAX_PARALLEL_THREADS; ++i) {
        if (wped_.at(i).second == id) {
            set_wped(i, {target, id});
            wp_lock_.unlock();
            return Status::OK;
        }
    }
    LOG(ERROR) << log_location_prefix << "unreachable path";
    wp_lock_.unlock();
    return Status::ERR_FATAL;
}

void wp_meta::display() {
    for (std::size_t i = 0; i < KVS_MAX_PARALLEL_THREADS; ++i) {
        if (get_wped_used().test(i)) {
            LOG(INFO) << "epoch:\t" << get_wped().at(i).first << ", id:\t"
                      << get_wped().at(i).second;
        }
    }
}

void wp_meta::init() {
    clear_wped();
    wped_used_.reset();
}

wp_meta::wped_type wp_meta::get_wped() {
    wped_type r_obj{};
    for (;;) {
        auto ts_f{wp_lock_.load_obj()};
        if (wp_lock::is_locked(ts_f)) {
            _mm_pause();
            continue;
        }
        r_obj = wped_;
        auto ts_s{wp_lock_.load_obj()};
        if (wp_lock::is_locked(ts_s) || ts_f != ts_s) { continue; }
        break;
    }
    return r_obj;
}

Status wp_meta::find_slot(std::size_t& at) {
    for (std::size_t i = 0; i < KVS_MAX_PARALLEL_THREADS; ++i) {
        if (!wped_used_.test(i)) {
            at = i;
            return Status::OK;
        }
    }
    LOG(ERROR) << log_location_prefix << "unreachable path";
    return Status::WARN_NOT_FOUND;
}

epoch::epoch_t wp_meta::find_min_ep(const wp_meta::wped_type& wped) {
    bool first{true};
    epoch::epoch_t min_ep{0};
    for (auto&& elem : wped) {
        if (elem.first != 0) {
            // used slot
            if (first) {
                first = false;
                min_ep = elem.first;
            } else if (min_ep > elem.first) {
                min_ep = elem.first;
            }
        }
    }
    return min_ep;
}

std::pair<epoch::epoch_t, std::size_t>
wp_meta::find_min_ep_id(const wp_meta::wped_type& wped) {
    bool first{true};
    std::size_t min_id{0};
    std::size_t min_ep{0};
    for (auto&& elem : wped) {
        if (elem.first != 0) {
            // used slot
            if (first) {
                first = false;
                min_ep = elem.first;
                min_id = elem.second;
            } else if (min_id > elem.second) {
                min_ep = elem.first;
                min_id = elem.second;
            }
        }
    }
    return {min_ep, min_id};
}

std::size_t wp_meta::find_min_id(const wp_meta::wped_type& wped) {
    bool first{true};
    std::size_t min_id{0};
    for (auto&& elem : wped) {
        if (elem.first != 0) {
            // used slot
            if (first) {
                first = false;
                min_id = elem.second;
            } else if (min_id > elem.second) {
                min_id = elem.second;
            }
        }
    }
    return min_id;
}

Status wp_meta::register_wp(epoch::epoch_t ep, std::size_t id) {
    wp_lock_.lock();
    std::size_t slot{};
    if (Status::OK != find_slot(slot)) { return Status::ERR_CC; }
    wped_used_.set(slot);
    set_wped(slot, {ep, id});
    wp_lock_.unlock();
    return Status::OK;
}

[[nodiscard]] Status
wp_meta::register_wp_result_and_remove_wp(wp_result_elem_type const& elem) {
    {
        std::lock_guard<std::shared_mutex> lk{mtx_wp_result_set_};
        wp_result_set_.emplace_back(elem);
    }
    wp_lock_.lock();
    return remove_wp_without_lock(wp_result_elem_extract_id(elem));
}

[[nodiscard]] Status wp_meta::remove_wp_without_lock(std::size_t const id) {
    for (std::size_t i = 0; i < KVS_MAX_PARALLEL_THREADS; ++i) {
        if (wped_.at(i).second == id) {
            set_wped(i, {0, 0});
            wped_used_.reset(i);
            wp_lock_.unlock();
            return Status::OK;
        }
    }
    return Status::WARN_NOT_FOUND;
}

} // namespace shirakami::wp