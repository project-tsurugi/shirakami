
#include <xmmintrin.h>

#include <string_view>

#include "clock.h"

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
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
    if (ti->find_high_priority_short() == Status::WARN_PREMATURE) {
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
    // 1: optimistic early check, 2: pessimistic check.
    // here, 1: optimistic early check
    for (;;) {
        wp::wp_meta* wp_meta_ptr{};
        if (wp::find_wp_meta(storage, wp_meta_ptr) != Status::OK) {
            return Status::WARN_STORAGE_NOT_FOUND;
        }
        auto wps = wp_meta_ptr->get_wped();
        if (wp::wp_meta::empty(wps)) { break; }
        auto ep_id{wp::wp_meta::find_min_ep_id(wps)};
        if (ep_id.second < ti->get_long_tx_id()) {
            // the wp is higher priority long tx than this.
            if (ti->get_read_version_max_epoch() > ep_id.first) {
                /** 
                  * If this tx try put before, old read operation of this will 
                  * be invalid. 
                  */
                shirakami::abort(ti); // or wait
                return Status::ERR_FAIL_WP;
            }
            // try put before
            // 2: pessimistic check
            {
                /**
                  * take lock: ongoing tx.
                  * If not coordinated with ongoing tx, the GC may delete 
                  * even the necessary information.
                  */
                std::lock_guard<std::shared_mutex> ongo_lk{
                        ongoing_tx::get_mtx()};
                /**
                  * verify ongoing tx is not changed.
                  */
                if (ongoing_tx::change_epoch_without_lock(
                            ti->get_long_tx_id(), ep_id.first, ep_id.second,
                            ep_id.first) != Status::OK) {
                    // Maybe it doesn't have to be prefixed.
                    continue;
                }
                // the high priori tx exists yet.
                ti->set_valid_epoch(ep_id.first);
                // change wp epoch
                change_wp_epoch(ti, ep_id.first);
                wp::extract_higher_priori_ltx_info(ti, wp_meta_ptr, wps);
            }
        }
        break;
    }

    // register read_by_set
    point_read_by_long* rbp{};
    auto rc = wp::find_read_by(storage, rbp);
    if (rc == Status::OK) {
        ti->get_point_read_by_long_set().emplace_back(rbp);
    } else {
        // todo. err_fatal programming error?
        return Status::WARN_NOT_FOUND;
    }

    // version function
    version* ver{};
    bool is_latest{false};
    tid_word f_check{};
    rc = version_function_with_optimistic_check(rec_ptr, ti->get_valid_epoch(),
                                                ver, is_latest, f_check);

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
         * function
         */
        version_function_without_optimistic_check(ti->get_valid_epoch(), ver);
    }

    // read non-latest version after version function
    if (read_value) { ver->get_value(value); }
    // check max epoch of read version
    auto read_epoch{ver->get_tid().get_epoch()};
    if (read_epoch > ti->get_read_version_max_epoch()) {
        ti->set_read_version_max_epoch(read_epoch);
    }
    return Status::OK;
}

} // namespace shirakami::long_tx