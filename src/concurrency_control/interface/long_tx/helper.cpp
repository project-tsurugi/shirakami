
#include "concurrency_control/include/long_tx.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"


namespace shirakami::long_tx {

Status change_wp_epoch(session* const ti, epoch::epoch_t const target) {
    for (auto&& elem : ti->get_wp_set()) {
        auto rc{elem.second->change_wp_epoch(ti->get_long_tx_id(), target)};
        if (rc != Status::OK) {
            LOG(ERROR) << "programming error";
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
    for (auto elem : ra.get_negative_list()) {
        ra.get_positive_list().erase(elem);
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
        LOG(ERROR) << "long tx id depletion. limit of specification.";
        return Status::ERR_FATAL;
    }
    ti->set_long_tx_id(long_tx_id);
    ti->set_valid_epoch(valid_epoch);
    ongoing_tx::push({valid_epoch, long_tx_id});

    // cut positive list by negative list.
    preprocess_read_area(ra);
    // set read area
    auto rc = set_read_plans(ti, ti->get_long_tx_id(), ra);
    if (rc != Status::OK) {
        long_tx::abort(ti);
        return Status::WARN_INVALID_ARGS;
    }
    ti->set_read_area(ra);

    // update metadata
    ti->set_requested_commit(false);

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

    LOG(ERROR) << "programming error";
    return Status::ERR_FATAL;
}

Status version_function_with_optimistic_check(Record* rec, epoch::epoch_t ep,
                                              version*& ver, bool& is_latest,
                                              tid_word& f_check) {
    // initialize
    is_latest = false;

    f_check = loadAcquire(&rec->get_tidw_ref().get_obj());

    if (f_check.get_lock() && f_check.get_latest() && f_check.get_absent()) {
        // until WP-2, it is not found because the inserter must be short tx.
        return Status::WARN_NOT_FOUND;
    }

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

Status wp_verify_and_forwarding(session* ti, wp::wp_meta* wp_meta_ptr,
                                const std::string_view read_info) {
    // 1: optimistic early check, 2: pessimistic check.
    // here, 1: optimistic early check
    for (;;) {
        auto wps = wp_meta_ptr->get_wped();
        if (wp::wp_meta::empty(wps)) { break; }
        auto ep_id{wp::wp_meta::find_min_ep_id(wps)};
        if (ep_id.second < ti->get_long_tx_id()) {
            // the wp is higher priority long tx than this.
            //if (ti->get_read_version_max_epoch() >= ep_id.first) {
            //    /** 
            //      * If this tx try put before, old read operation of this will 
            //      * be invalid. 
            //      */
            //    long_tx::abort(ti); // or wait
            //    return Status::ERR_FAIL_WP;
            //}
            // try put before
            // 2: pessimistic check
            {
                ///**
                //  * take lock: ongoing tx.
                //  * If not coordinated with ongoing tx, the GC may delete 
                //  * even the necessary information.
                //  */
                //std::lock_guard<std::shared_mutex> ongo_lk{
                //        ongoing_tx::get_mtx()};
                ///**
                //  * verify ongoing tx is not changed.
                //  */
                //if (ongoing_tx::change_epoch_without_lock(
                //            ti->get_long_tx_id(), ep_id.first, ep_id.second,
                //            ep_id.first) != Status::OK) {
                //    // Maybe it doesn't have to be prefixed.
                //    continue;
                //}
                //// the high priori tx exists yet.
                //ti->set_valid_epoch(ep_id.first);
                //// change wp epoch
                //change_wp_epoch(ti, ep_id.first);
                wp::extract_higher_priori_ltx_info(ti, wp_meta_ptr, wps,
                                                   read_info);
            }
        }
        break;
    }

    return Status::OK;
}

} // namespace shirakami::long_tx