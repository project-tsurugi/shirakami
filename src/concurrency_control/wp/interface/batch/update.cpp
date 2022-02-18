

#include "concurrency_control/wp/include/local_set.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "yakushima/include/kvs.h"

namespace shirakami::batch {

Status update([[maybe_unused]] session* ti, [[maybe_unused]] Storage storage,
              [[maybe_unused]] const std::string_view key,
              [[maybe_unused]] const std::string_view val) {
    // checks
    if (ti->get_mode() == tx_mode::BATCH &&
        epoch::get_global_epoch() < ti->get_valid_epoch()) {
        // not in valid epoch.
        return Status::WARN_PREMATURE;
    }
    if (ti->get_mode() == tx_mode::BATCH && !ti->check_exist_wp_set(storage)) {
        // can't write without wp.
        return Status::WARN_INVALID_ARGS;
    }

RETRY_INDEX_ACCESS:

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
    }

    auto rc{insert_process(ti, storage, key, val)};
    if (rc == Status::WARN_CONCURRENT_INSERT) {
        goto RETRY_INDEX_ACCESS; // NOLINT
    } else {
        return rc;
    }
}

} // namespace shirakami::batch