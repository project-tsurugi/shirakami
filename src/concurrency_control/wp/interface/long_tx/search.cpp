
#include <xmmintrin.h>

#include <string_view>

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/version.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::long_tx {

Status search_key(session* ti, Storage const storage,
                  std::string_view const key, std::string& value,
                  bool const read_value) {
    if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }

    if (garbage::get_min_step_epoch() <= ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
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
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    // wp verify
    auto wps = wp::find_wp(storage);
    if (!wp::wp_meta::empty(wps) &&
        wp::wp_meta::find_min_id(wps) != ti->get_batch_id()) {
        abort(ti); // or wait
        /**
         * because: You have to wait for the end of the transaction to read 
         * the prefixed batch write.
         * 
         */
        return Status::ERR_FAIL_WP;
    }

    // register read_by_set
    read_by_bt* rbp{};
    auto rc = wp::find_read_by(storage, rbp);
    if (rc == Status::OK) {
        ti->get_read_by_bt_set().emplace_back(rbp);
    } else {
        return Status::WARN_NOT_FOUND;
    }

VER_SELEC:
    // version selection
    version* ver{};
    tid_word f_check{loadAcquire(&rec_ptr->get_tidw_ref().get_obj())};
    /**
     * todo enhancement: not wait lock release and optimistic read or 
     * deterministic read.
     */
    for (;;) {
        if (f_check.get_lock()) {
            _mm_pause();
            f_check = loadAcquire(&rec_ptr->get_tidw_ref().get_obj());
            continue;
        }
        ver = rec_ptr->get_latest();
        tid_word s_check{loadAcquire(&rec_ptr->get_tidw_ref().get_obj())};
        if (s_check.get_lock()) {
            _mm_pause();
            f_check = loadAcquire(&rec_ptr->get_tidw_ref().get_obj());
            continue;
        }
        if (f_check == s_check) {
            // Whatever value tid is, ver is the latest version.
            break;
        }
        _mm_pause();
        f_check = s_check;
    }

    if (ti->get_valid_epoch() > f_check.get_epoch()) {
        if (read_value) { ver->get_value(value); }
        if (ver == rec_ptr->get_latest() &&
            loadAcquire(&rec_ptr->get_tidw_ref().get_obj()) !=
                    f_check.get_obj()) {
            // read latest version and fail optimistic read.
            goto VER_SELEC; // NOLINT
        }
        // read latest version
        return Status::OK;
    }

    // read non latest version
    for (;;) {
        ver = ver->get_next();
        if (ver == nullptr) { LOG(FATAL) << "unreachable"; }

        if (ti->get_valid_epoch() > ver->get_tid().get_epoch()) {
            if (read_value) { ver->get_value(value); }
            return Status::OK;
        }
    }
}

} // namespace shirakami::long_tx