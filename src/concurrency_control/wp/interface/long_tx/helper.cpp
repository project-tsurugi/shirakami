
#include "concurrency_control/wp/include/long_tx.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

#include "concurrency_control/include/tuple_local.h"

namespace shirakami::long_tx {

Status change_wp_epoch(session* const ti, epoch::epoch_t const target) {
    for (auto&& elem : ti->get_wp_set()) {
        auto rc{elem.second->change_wp_epoch(ti->get_batch_id(), target)};
        if (rc != Status::OK) {
            LOG(ERROR) << "programming error";
            return rc;
        }
    }
    return Status::OK;
}

Status tx_begin(session* const ti,
                std::vector<Storage> write_preserve) { // NOLINT
    // get wp mutex
    auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());

    // get batch id
    auto batch_id = shirakami::wp::long_tx::get_counter();

    // compute future epoch
    auto valid_epoch = epoch::get_global_epoch() + 1;

    // do write preserve
    if (!write_preserve.empty()) {
        auto rc{wp::write_preserve(ti, std::move(write_preserve), batch_id,
                                   valid_epoch)};
        if (rc != Status::OK) { return rc; }
    }

    // inc batch counter
    // after deciding success
    wp::long_tx::set_counter(batch_id + 1);

    ti->set_batch_id(batch_id);
    ongoing_tx::push({valid_epoch, batch_id});
    ti->set_tx_type(TX_TYPE::LONG);
    ti->set_valid_epoch(valid_epoch);

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

} // namespace shirakami::long_tx