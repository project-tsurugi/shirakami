
#include <string_view>

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/version.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami::occ {

Status search_key(session* ti, Storage const storage,
                  std::string_view const key, Tuple*& tuple) {
    // index access
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<const char*>(&storage), // NOLINT
             sizeof(storage)},                        // NOLINT
            key))};
    if (rec_d_ptr == nullptr) {
        tuple = nullptr;
        return Status::WARN_NOT_FOUND;
    }
    Record* rec_ptr{*rec_d_ptr};

    // check local write set
    write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (in_ws != nullptr) {
        if (in_ws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        ti->get_cache_for_search_ptr()->get_pimpl()->set_key(
                in_ws->get_rec_ptr()->get_key());
        ti->get_cache_for_search_ptr()->get_pimpl()->set_val(in_ws->get_val());
        tuple = ti->get_cache_for_search_ptr();
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    // wp check
    wp::wp_meta::wped_type wps{find_wp(storage)};
    if (!wps.empty()) {
        abort(ti);
        return Status::ERR_VALIDATION;
    }
    tid_word read_tid;
    std::string* read_val{};
    // read version
    Status rs{read_record(rec_ptr, read_tid, read_val)};
    if (rs == Status::OK) {
        ti->get_read_set().emplace_back(storage, rec_ptr, read_tid);
        ti->get_cache_for_search_ptr()->get_pimpl()->set_key(key);
        ti->get_cache_for_search_ptr()->get_pimpl()->set_val(*read_val);
        tuple = ti->get_cache_for_search_ptr();
    } else {
        tuple = nullptr;
    }
    return rs;
}

} // namespace shirakami::occ