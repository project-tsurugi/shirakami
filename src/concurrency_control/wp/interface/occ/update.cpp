
#include "concurrency_control/wp/include/local_set.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

namespace shirakami::occ {

Status update(session* ti, Storage storage, const std::string_view key,
              const std::string_view val) {
    // index access to check local write set
    Record* rec_ptr{};
    if (Status::OK == get<Record>(storage, key, rec_ptr)) {
        // check local write
        write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
        if (in_ws != nullptr) {
            if (in_ws->get_op() == OP_TYPE::INSERT) {
                in_ws->get_rec_ptr()->get_latest()->set_value(val);
            } else {
                in_ws->set_op(OP_TYPE::UPSERT);
                in_ws->set_val(val);
            }
            return Status::WARN_WRITE_TO_LOCAL_WRITE;
        }

        // prepare write
        ti->get_write_set().push(
                {storage, OP_TYPE::UPSERT, rec_ptr, val}); // NOLINT
        return Status::OK;
    } else {
        return Status::WARN_NOT_FOUND;
    }
}

} // namespace shirakami::occ