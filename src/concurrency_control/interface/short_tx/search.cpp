
#include <string_view>

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami::short_tx {

inline Status wp_verify(session* const ti, Storage const st) {
    wp::wp_meta* wm{};
    auto rc{wp::find_wp_meta(st, wm)};
    if (rc != Status::OK) { return Status::WARN_STORAGE_NOT_FOUND; }
    auto wps{wm->get_wped()};
    auto find_min_ep{wp::wp_meta::find_min_ep(wps)};
    if (find_min_ep != 0 && find_min_ep <= epoch::get_global_epoch()) {
        std::unique_lock<std::mutex> lk{ti->get_mtx_termination()};
        short_tx::abort(ti);
        ti->set_result(reason_code::CC_OCC_WP_VERIFY);
        return Status::ERR_CC;
    }
    return Status::OK;
}

Status search_key(session* ti, Storage const storage,
                  std::string_view const key, std::string& value,
                  bool const read_value) {
    // check wp
    auto rc{wp_verify(ti, storage)};
    if (rc != Status::OK) { return rc; }

    // index access
    Record* rec_ptr{};
    std::pair<yakushima::node_version64_body, yakushima::node_version64*>
            checked_version{};
    rc = get<Record>(storage, key, rec_ptr, &checked_version);
    if (rc != Status::OK) {
        // read protection for concurrent occ
        auto rc_ns = ti->get_node_set().emplace_back(checked_version);
        if (rc_ns == Status::ERR_CC) {
            short_tx::abort(ti);
            std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
            ti->get_result_info().set_storage_name(storage);
            ti->set_result(reason_code::CC_OCC_PHANTOM_AVOIDANCE);
            return Status::ERR_CC;
        }

        // read protection for low priori ltx
        wp::page_set_meta* psm{};
        auto rc_fpsm{wp::find_page_set_meta(storage, psm)};
        if (rc_fpsm == Status::WARN_NOT_FOUND) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return Status::ERR_FATAL;
        }
        range_read_by_short* rrbs{psm->get_range_read_by_short_ptr()};
        ti->push_to_range_read_by_short_set(rrbs);
        return rc;
    }

    // check local write set
    write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (in_ws != nullptr) {
        if (in_ws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_NOT_FOUND;
        }
        if (read_value) {
            std::shared_lock<std::shared_mutex> lk{rec_ptr->get_mtx_value()};
            in_ws->get_value(value);
        }
        return Status::OK;
    }

    tid_word read_tid{};
    std::string read_res{};
    // read version
    Status rs{read_record(rec_ptr, read_tid, read_res, read_value)};
    // it didn't read by others lock.
    if (rs == Status::WARN_CONCURRENT_UPDATE) { return rs; }
    // it did tx read.
    if (rc == Status::OK) {
        // it read normal page
        if (read_value) { value = read_res; }
    }
    ti->push_to_read_set_for_stx({storage, rec_ptr, read_tid});
    return rs;
}

} // namespace shirakami::short_tx
