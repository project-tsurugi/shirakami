
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

Status check_read_area(session* ti, Storage st) {
    // check positive list
    if (!ti->get_read_area().get_positive_list().empty()) {
        bool is_found{false};
        for (auto elem : ti->get_read_area().get_positive_list()) {
            if (elem == st) {
                is_found = true;
                // it can break
                break;
            }
        }
        if (!is_found) { return Status::ERR_READ_AREA_VIOLATION; }
        // success verify about positive list
    }

    // check negative list
    if (!ti->get_read_area().get_negative_list().empty()) {
        bool is_found{false};
        for (auto elem : ti->get_read_area().get_negative_list()) {
            if (elem == st) {
                is_found = true;
                // it can break
                break;
            }
        }
        if (is_found) { return Status::ERR_READ_AREA_VIOLATION; }
        // success verify about negative list
    }

    return Status::OK;
}

void preprocess_read_area(transaction_options::read_area& ra) {
    std::vector<Storage> st_list{};
    storage::list_storage(st_list);

    // if you don't set positive / negative, you may read all.
    if (ra.get_positive_list().empty() && ra.get_negative_list().empty()) {
        for (auto elem : st_list) { ra.insert_to_positive_list(elem); }

        return;
    }

    // if you set positive only, you can read that only.
    // no work to need

    // if you set negative only, you can read other than negative
    if (ra.get_positive_list().empty() && !ra.get_negative_list().empty()) {
        // register all to positive and remove by negative
        for (auto elem : st_list) { ra.insert_to_positive_list(elem); }
    }

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
    auto rc = set_read_plans(ti, ti->get_long_tx_id(), ra);
    if (rc != Status::OK) {
        long_tx::abort(ti);
        return Status::WARN_INVALID_ARGS;
    }
    // 読みうるものは必ず read positive として登録する。
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
                              const std::string_view read_info) {
    auto wps = wp_meta_ptr->get_wped();
    if (!wp::wp_meta::empty(wps)) {
        // exist wp
        auto ep_id{wp::wp_meta::find_min_ep_id(wps)};
        if (ep_id.second < ti->get_long_tx_id()) {
            // the wp is higher priority long tx than this.
            wp::extract_higher_priori_ltx_info(ti, wp_meta_ptr, wps, read_info);
        }
    }
}

} // namespace shirakami::long_tx