
#include <algorithm>

#include "storage.h"

#include "concurrency_control/include/long_tx.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "database/include/logging.h"

namespace shirakami::long_tx {

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
