
#include <xmmintrin.h>

#include <string_view>

#include "clock.h"

#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/include/helper.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "database/include/logging.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::long_tx {

static void set_read_version_max_epoch_if_need(session* ti, epoch::epoch_t ep) {
    if (ti->get_tx_type() != transaction_options::transaction_type::READ_ONLY) {
        ti->set_read_version_max_epoch_if_need(ep);
    }
}

/**
 * @return Status::WARN_NOT_FOUND
 * @return Status::OK
 */
extern Status version_traverse_and_read(session* const ti,
                                        Record* const rec_ptr,
                                        std::string& value,
                                        bool const read_value) {
RETRY:
    // version function
    version* ver = nullptr;
    bool is_latest = false;
    tid_word f_check = {};
    auto rc = version_function_with_optimistic_check(
            rec_ptr, ti->get_valid_epoch(), ver, is_latest, f_check);

    if (rc == Status::WARN_NOT_FOUND) { return rc; }
    if (rc != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }

    // read latest version after version function
    if (is_latest) {
        if (!f_check.get_absent()) {
            if (read_value) { ver->get_value(value); }
        }
        if (ver == rec_ptr->get_latest() &&
            loadAcquire(&rec_ptr->get_tidw_ref().get_obj()) ==
                    f_check.get_obj()) {
            // success optimistic read latest version
            // check max epoch of read version
            auto read_epoch{f_check.get_epoch()};
            if (read_epoch > ti->get_read_version_max_epoch()) {
                set_read_version_max_epoch_if_need(ti, read_epoch);
            }
            if (f_check.get_absent()) { return Status::WARN_NOT_FOUND; }
            return Status::OK;
        }
        /**
         * else: fail to do optimistic read latest version. retry version
         * function.
         * The latest version may be the version which this tx should read,
         * so this tx must wait unlocking because it may read invalid state with
         * locking.
         */
        goto RETRY; // NOLINT
    }

    // read non-latest version after version function
    if (ver == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
    }
    if (!ver->get_tid().get_absent()) {
        if (read_value) { ver->get_value(value); }
    }
    // check max epoch of read version
    auto read_epoch{ver->get_tid().get_epoch()};
    if (read_epoch > ti->get_read_version_max_epoch()) {
        set_read_version_max_epoch_if_need(ti, read_epoch);
    }
    return ver->get_tid().get_absent() ? Status::WARN_NOT_FOUND : Status::OK;
}

} // namespace shirakami::long_tx
