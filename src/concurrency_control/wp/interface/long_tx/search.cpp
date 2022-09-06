
#include <xmmintrin.h>

#include <string_view>

#include "clock.h"

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/version.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/include/helper.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::long_tx {

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
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    // read latest version after version function
    if (is_latest) {
        if (read_value) { ver->get_value(value); }
        if (ver == rec_ptr->get_latest() &&
            loadAcquire(&rec_ptr->get_tidw_ref().get_obj()) ==
                    f_check.get_obj()) {
            // success optimistic read latest version
            // check max epoch of read version
            auto read_epoch{f_check.get_epoch()};
            if (read_epoch > ti->get_read_version_max_epoch()) {
                ti->set_read_version_max_epoch(read_epoch);
            }
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
    if (ver == nullptr) { LOG(ERROR) << "programming error"; }
    if (read_value) { ver->get_value(value); }
    // check max epoch of read version
    auto read_epoch{ver->get_tid().get_epoch()};
    if (read_epoch > ti->get_read_version_max_epoch()) {
        ti->set_read_version_max_epoch(read_epoch);
    }
    return Status::OK;
}

Status search_key(session* ti, Storage const storage,
                  std::string_view const key, std::string& value,
                  bool const read_value) {
    // check start epoch
    if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }
    // wait for high priority some tx
    if (ti->find_high_priority_short() == Status::WARN_PREMATURE) {
        return Status::WARN_PREMATURE;
    }
    // check for read area invalidation
    auto rs = check_read_area(ti, storage);
    if (rs == Status::ERR_READ_AREA_VIOLATION) {
        long_tx::abort(ti);
        return rs;
    }


    // index access
    Record* rec_ptr{};
    if (Status::WARN_NOT_FOUND == get<Record>(storage, key, rec_ptr)) {
        return Status::WARN_NOT_FOUND;
    }

    // check local write set
    write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (in_ws != nullptr) {
        if (in_ws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        if (read_value) { in_ws->get_value(value); }
        return Status::OK;
    }

    // check storage existence and extract wp meta info
    wp::wp_meta* wp_meta_ptr{};
    if (wp::find_wp_meta(storage, wp_meta_ptr) != Status::OK) {
        return Status::WARN_STORAGE_NOT_FOUND;
    }

    // wp verify and forwarding
    auto rc = wp_verify_and_forwarding(ti, wp_meta_ptr);
    if (rc != Status::OK) { return rc; }

    // register read_by_set
    point_read_by_long* rbp{};
    rc = wp::find_read_by(storage, rbp);
    if (rc == Status::OK) {
        ti->get_point_read_by_long_set().insert(rbp);
    } else {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    return version_traverse_and_read(ti, rec_ptr, value, read_value);
}

} // namespace shirakami::long_tx