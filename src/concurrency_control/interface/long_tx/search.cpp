
#include <xmmintrin.h>

#include <string_view>

#include "clock.h"

#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/version.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/include/helper.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

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
        LOG(ERROR) << log_location_prefix << "unreachable path";
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
                ti->set_read_version_max_epoch(read_epoch);
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
        LOG(ERROR) << log_location_prefix << "unreachable path";
    }
    if (!ver->get_tid().get_absent()) {
        if (read_value) { ver->get_value(value); }
    }
    // check max epoch of read version
    auto read_epoch{ver->get_tid().get_epoch()};
    if (read_epoch > ti->get_read_version_max_epoch()) {
        ti->set_read_version_max_epoch(read_epoch);
    }
    return Status::OK;
}

static void create_read_set_for_read_info(session* const ti,
                                          Record* const rec_ptr) {
    // register read_by_set
    ti->read_set_for_ltx().push(rec_ptr);
}

static Status check_before_execution(session* const ti, Storage const storage) {
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
        std::unique_lock<std::mutex> lk{ti->get_mtx_termination()};
        long_tx::abort(ti);
        ti->set_result(reason_code::CC_LTX_READ_AREA_VIOLATION);
        return Status::ERR_CC;
    }

    return Status::OK;
}

static Status hit_local_write_set(write_set_obj* const in_ws, Record* rec_ptr,
                                  std::string& value, bool const read_value) {
    if (in_ws->get_op() == OP_TYPE::DELETE) { return Status::WARN_NOT_FOUND; }
    if (read_value) {
        std::shared_lock<std::shared_mutex> lk{rec_ptr->get_mtx_value()};
        in_ws->get_value(value);
    }
    return Status::OK;
}

Status search_key(session* ti, Storage const storage,
                  std::string_view const key, std::string& value,
                  bool const read_value) {
    auto rc = check_before_execution(ti, storage);
    if (rc != Status::OK) { return rc; }

    // index access
    Record* rec_ptr{};
    if (Status::WARN_NOT_FOUND == get<Record>(storage, key, rec_ptr)) {
        return Status::WARN_NOT_FOUND;
    }

    // check local write set
    write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (in_ws != nullptr) {
        rc = hit_local_write_set(in_ws, rec_ptr, value, read_value);
        if (rc == Status::OK) {
            if (in_ws->get_op() != OP_TYPE::UPSERT) {
                // note: read own upsert don't need to log read info.
                create_read_set_for_read_info(ti, rec_ptr);
            }
            return rc;
        }
        if (rc == Status::WARN_NOT_FOUND) { return rc; }
    }

    // check storage existence and extract wp meta info
    wp::wp_meta* wp_meta_ptr{};
    if (wp::find_wp_meta(storage, wp_meta_ptr) != Status::OK) {
        return Status::WARN_STORAGE_NOT_FOUND;
    }

    // wp verify and forwarding
    wp_verify_and_forwarding(ti, wp_meta_ptr, key);

    rc = version_traverse_and_read(ti, rec_ptr, value, read_value);
    if (rc == Status::OK) { create_read_set_for_read_info(ti, rec_ptr); }
    return rc;
}

} // namespace shirakami::long_tx