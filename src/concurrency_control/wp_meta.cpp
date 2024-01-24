
#include "concurrency_control/include/wp_meta.h"

#include "shirakami/logging.h"

#include "glog/logging.h"

namespace shirakami::wp {

bool wp_meta::empty(const wp_meta::wped_type& wped) {
    return std::all_of(
            wped.begin(), wped.end(), [](wp::wp_meta::wped_elem_type elem) {
                return elem == std::pair<epoch::epoch_t, std::size_t>(0, 0);
            });
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
    auto ret = remove_wp_without_lock(wp_result_elem_extract_id(elem));
    if (ret == Status::OK) { return ret; }
    wp_lock_.unlock();

    // clear wp write range info
    remove_write_range(std::get<1>(elem));
    return ret;
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
    LOG(ERROR) << log_location_prefix << "concurrent program error";
    return Status::WARN_NOT_FOUND;
}

void wp_meta::push_write_range(std::size_t txid, std::string_view left_key,
                               std::string_view right_key) {
    std::lock_guard<std::shared_mutex> lk{get_mtx_write_range()};

    auto ret_pair = get_write_range().insert(
            std::make_pair(txid, std::make_tuple(left_key, right_key)));
    if (!ret_pair.second) {
        // already exist, not inserted
        LOG(ERROR) << log_location_prefix
                   << "programming error. tx do this only once.";
    }
}

void wp_meta::remove_write_range(std::size_t const txid) {
    std::lock_guard<std::shared_mutex> lk{get_mtx_write_range()};

    auto ret_num = get_write_range().erase(txid);
    if (ret_num != 1) {
        // can't erase
        LOG(ERROR) << log_location_prefix
                   << "programming error. it does once after push_write_range";
    }
}

bool wp_meta::read_write_range(std::size_t txid, std::string_view& out_left_key,
                               std::string_view& out_right_key) {
    std::shared_lock<std::shared_mutex> lk{get_mtx_write_range()};

    auto ret_itr = get_write_range().find(txid);
    if (ret_itr == get_write_range().end()) { // not found
        return false;
    } // found
    out_left_key = std::get<0>(ret_itr->second);
    out_right_key = std::get<1>(ret_itr->second);
    return true;
}

} // namespace shirakami::wp