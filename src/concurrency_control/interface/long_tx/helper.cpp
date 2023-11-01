
#include <algorithm>

#include "storage.h"

#include "concurrency_control/include/long_tx.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "database/include/logging.h"

namespace shirakami::long_tx {

Status change_wp_epoch(session* const ti, epoch::epoch_t const target) {
    for (auto&& elem : ti->get_wp_set()) {
        auto rc{elem.second->change_wp_epoch(ti->get_long_tx_id(), target)};
        if (rc != Status::OK) {
            LOG(ERROR) << log_location_prefix << "unreachable path";
            return rc;
        }
    }
    return Status::OK;
}

Status check_read_area_body(session* ti, Storage const st) {
    auto ra = ti->get_read_area();
    auto plist = ra.get_positive_list();
    auto nlist = ra.get_negative_list();

    // cond 1 empty and empty
    if (plist.empty() && nlist.empty()) { return Status::OK; }

    // cond 3 only nlist
    if (plist.empty()) {
        // nlist is not empty
        auto itr = nlist.find(st);
        if (itr != nlist.end()) { return Status::ERR_READ_AREA_VIOLATION; }
        return Status::OK;
    }

    // cond 2 only plist and cond 4 p and n
    // it can read from only plist
    // plist is not empty
    auto itr = plist.find(st);
    if (itr != plist.end()) { // found
        return Status::OK;
    }
    return Status::ERR_READ_AREA_VIOLATION;
}

Status check_read_area(session* ti, Storage const st) {
    auto ret = check_read_area_body(ti, st);
    if (ret == Status::OK) {
        // log read storage
        ti->insert_to_ltx_storage_read_set(st);
    }
    return ret;
}

void preprocess_read_area(transaction_options::read_area& ra) {
    if (!ra.get_positive_list().empty() && !ra.get_negative_list().empty()) {
        // if you set positive and negative, you can read positive erased by negative
        for (auto elem : ra.get_negative_list()) {
            auto pset = ra.get_positive_list();
            for (auto itr = pset.begin(); itr != pset.end(); ++itr) { // NOLINT
                if (elem == *itr) {
                    ra.erase_from_positive_list(elem);
                    break;
                }
            }
        }
    }
}

void update_wp_at_commit(session* const ti, std::set<Storage> const& sts) {
    /**
     * write preserve はTX開始時点に宣言したものよりも実体の方が同じか小さくなる。
     * 小さくできるなら小さくすることで、他Txへの影響を軽減する。
     * */
    for (auto itr = ti->get_wp_set().begin(); itr != ti->get_wp_set().end();) {
        // check the storage is valid yet
        Storage target_st{itr->first};
        wp::page_set_meta* target_psm_ptr{};
        auto ret = wp::find_page_set_meta(target_st, target_psm_ptr);
        if (ret != Status::OK ||
            // check the ptr was not changed
            (ret == Status::OK &&
             itr->second != target_psm_ptr->get_wp_meta_ptr())) {
            LOG(ERROR) << log_location_prefix
                       << "Error. Suspected mix of DML and DDL";
            ++itr;
            continue;
        }

        bool hit_actual{false};
        for (auto actual_write_storage : sts) {
            if (actual_write_storage == itr->first) {
                // exactly, write this storage
                hit_actual = true;
                break;
            }
        }
        if (hit_actual) {
            ++itr;
            continue;
        } // else
          /**
         * wp したが、実際には書かなかったストレージである。コミット処理前にこれを
         * 取り除く
        */
        {
            itr->second->get_wp_lock().lock();
            ret = itr->second->remove_wp_without_lock(ti->get_long_tx_id());
            if (ret == Status::OK) {
                // 縮退成功
                itr = ti->get_wp_set().erase(itr);
                continue;
            }
            LOG(ERROR) << log_location_prefix << "unexpected code path";
            itr->second->get_wp_lock().unlock();
        }
        ++itr;
    }
}

Status tx_begin(session* const ti, std::vector<Storage> write_preserve,
                transaction_options::read_area ra) { // NOLINT
    // get wp mutex, exclude long tx's coming and epoch update
    auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());

    // get long tx id
    auto long_tx_id = shirakami::wp::long_tx::get_counter();

    // compute future epoch
    auto valid_epoch = epoch::get_global_epoch() + 1;

    // do write preserve
    if (!write_preserve.empty()) {
        auto rc{wp::write_preserve(ti, std::move(write_preserve), long_tx_id,
                                   valid_epoch)};
        if (rc != Status::OK) { return rc; }
    }

    // inc batch counter
    // after deciding success
    wp::long_tx::set_counter(long_tx_id + 1);

    if (long_tx_id >= pow(2, 63)) { // NOLINT
        LOG(ERROR) << log_location_prefix
                   << "long tx id depletion. limit of specification.";
        return Status::ERR_FATAL;
    }
    ti->set_long_tx_id(long_tx_id);
    ti->set_valid_epoch(valid_epoch);
    ongoing_tx::push({valid_epoch, long_tx_id, ti});

    // cut positive list by negative list.
    preprocess_read_area(ra);
    // set read area
    read_plan::add_elem(ti->get_long_tx_id(), ra);
    ti->set_read_area(ra);

    // update metadata
    ti->set_requested_commit(false);

    // detail info
    if (logging::get_enable_logging_detail_info()) {
        VLOG(log_trace) << log_location_prefix_detail_info
                        << "tx_begin, LTX, tx id: " << long_tx_id;
    }

    return Status::OK;
    // dtor : release wp_mutex
}

Status version_function_without_optimistic_check(epoch::epoch_t ep,
                                                 version*& ver) {
    for (;;) {
        ver = ver->get_next();
        if (ver == nullptr) { return Status::WARN_NOT_FOUND; }

        if (ep > ver->get_tid().get_epoch()) { return Status::OK; }
    }

    LOG(ERROR) << log_location_prefix << "unreachable path";
    return Status::ERR_FATAL;
}

Status version_function_with_optimistic_check(Record* rec, epoch::epoch_t ep,
                                              version*& ver, bool& is_latest,
                                              tid_word& f_check) {
    // initialize
    is_latest = false;

    f_check = loadAcquire(&rec->get_tidw_ref().get_obj());

    for (;;) {
        if (f_check.get_lock()) {
            /**
             * not inserting records and the owner may be escape the value 
             * which is the target for this tx.
             */
            _mm_pause();
            f_check = loadAcquire(&rec->get_tidw_ref().get_obj());
            continue;
        }
        break;
    }
    // here, the target for this tx must be escaped.

    ver = rec->get_latest();

    if (ep > f_check.get_epoch()) {
        is_latest = true;
        return Status::OK;
    }

    return version_function_without_optimistic_check(ep, ver);
}

void wp_verify_and_forwarding(session* ti, wp::wp_meta* wp_meta_ptr,
                              const std::string_view read_key) {
    auto wps = wp_meta_ptr->get_wped();
    if (!wp::wp_meta::empty(wps)) {
        // exist wp
        auto ep_id{wp::wp_meta::find_min_ep_id(wps)};
        if (ep_id.second < ti->get_long_tx_id()) {
            // the wp is higher priority long tx than this.
            wp::extract_higher_priori_ltx_info(ti, wp_meta_ptr, wps, read_key);
        }
    }
}

void wp_verify_and_forwarding(session* ti, wp::wp_meta* wp_meta_ptr) {
    auto wps = wp_meta_ptr->get_wped();
    if (!wp::wp_meta::empty(wps)) {
        // exist wp
        auto ep_id{wp::wp_meta::find_min_ep_id(wps)};
        if (ep_id.second < ti->get_long_tx_id()) {
            // the wp is higher priority long tx than this.
            wp::extract_higher_priori_ltx_info(ti, wp_meta_ptr, wps);
        }
    }
}

void update_local_read_range(session* ti, wp::wp_meta* wp_meta_ptr,
                             const std::string_view key) {
    // get mutex
    std::lock_guard<std::shared_mutex> lk{ti->get_mtx_overtaken_ltx_set()};

    auto& read_range = std::get<1>(ti->get_overtaken_ltx_set()[wp_meta_ptr]);
    if (!std::get<4>(read_range)) {
        // it was not initialized
        std::get<0>(read_range) = key;
        std::get<2>(read_range) = key;
        std::get<4>(read_range) = true;
    } else {
        // it was initialized
        if (key < std::get<0>(read_range)) {
            std::get<0>(read_range) = key;
        } else if (key > std::get<2>(read_range)) {
            std::get<2>(read_range) = key;
        }
    }
}

void update_local_read_range(session* ti, wp::wp_meta* wp_meta_ptr,
                             std::string_view l_key, scan_endpoint l_end) {
    // get mutex
    std::lock_guard<std::shared_mutex> lk{ti->get_mtx_overtaken_ltx_set()};

    auto& read_range = std::get<1>(ti->get_overtaken_ltx_set()[wp_meta_ptr]);
    if (!std::get<4>(read_range)) {
        // it was not initialized
        std::get<4>(read_range) = true;
    }
    if (l_key < std::get<0>(read_range)) { std::get<0>(read_range) = l_key; }
    if (l_end == scan_endpoint::INF) { std::get<1>(read_range) = l_end; }
}

void update_local_read_range(session* ti, wp::wp_meta* wp_meta_ptr,
                             std::string_view l_key, scan_endpoint l_end,
                             std::string_view r_key, scan_endpoint r_end) {
    // get mutex
    std::lock_guard<std::shared_mutex> lk{ti->get_mtx_overtaken_ltx_set()};

    auto& read_range = std::get<1>(ti->get_overtaken_ltx_set()[wp_meta_ptr]);
    if (!std::get<4>(read_range)) {
        // it was not initialized
        std::get<4>(read_range) = true;
    }
    if (l_key < std::get<0>(read_range)) { std::get<0>(read_range) = l_key; }
    if (l_end == scan_endpoint::INF) { std::get<1>(read_range) = l_end; }
    if (std::get<2>(read_range) < r_key) { std::get<2>(read_range) = r_key; }
    if (r_end == scan_endpoint::INF) { std::get<3>(read_range) = r_end; }
}

} // namespace shirakami::long_tx