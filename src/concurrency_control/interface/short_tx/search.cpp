
#include <string_view>

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
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
    if (find_min_ep != 0 && find_min_ep <= ti->get_step_epoch()) {
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
    rc = get<Record>(storage, key, rec_ptr);
    if (rc != Status::OK) { return rc; }

    // check local write set
    write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (in_ws != nullptr) {
        if (in_ws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        if (read_value) { in_ws->get_value(value); }
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
    ti->get_read_set().emplace_back(storage, rec_ptr, read_tid);
    return rs;
}

} // namespace shirakami::short_tx