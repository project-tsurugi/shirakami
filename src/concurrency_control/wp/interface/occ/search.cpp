
#include <string_view>

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/version.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami::occ {

Status search_key(session* ti, Storage const storage,
                  std::string_view const key, std::string& value,
                  bool const read_value) {
    // index access
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<const char*>(&storage), // NOLINT
             sizeof(storage)},                        // NOLINT
            key))};
    if (rec_d_ptr == nullptr) {
        return Status::WARN_NOT_FOUND;
    }
    Record* rec_ptr{*rec_d_ptr};

    // check local write set
    write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (in_ws != nullptr) {
        if (in_ws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        if (read_value) { in_ws->get_value(value); }
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    tid_word read_tid{};
    std::string read_res{};
    // read version
    Status rs{read_record(rec_ptr, read_tid, read_res, read_value)};
    if (rs == Status::OK) {
        if (read_value) {
            ti->get_read_set().emplace_back(storage, rec_ptr, read_tid);
            value = read_res;
        }
    }
    return rs;
}

} // namespace shirakami::occ