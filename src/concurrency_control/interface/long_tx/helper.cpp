
#include <algorithm>

#include "storage.h"

#include "concurrency_control/include/long_tx.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "database/include/logging.h"

namespace shirakami::long_tx {

static Status check_read_area_body(session* ti, Storage const st) {
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

Status version_function_without_optimistic_check(epoch::epoch_t ep,
                                                 version*& ver) {
    for (;;) {
        ver = ver->get_next();
        if (ver == nullptr) { return Status::WARN_NOT_FOUND; }

        if (ep > ver->get_tid().get_epoch()) { return Status::OK; }
    }

    LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
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

} // namespace shirakami::long_tx
