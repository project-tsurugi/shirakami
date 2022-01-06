

#include "concurrency_control/wp/include/local_set.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"

#include "yakushima/include/kvs.h"

namespace shirakami::batch {

Status insert_process(session* const ti, Storage storage,
                      const std::string_view key, const std::string_view val) {
    // try insert
    Record* rec_ptr = new Record(key, val); // NOLINT
    yakushima::node_version64* nvp{};
    yakushima::status insert_result{yakushima::put<Record*>(
            ti->get_yakushima_token(),
            {reinterpret_cast<char*>(&storage), sizeof(storage)},      // NOLINT
            key, &rec_ptr, sizeof(Record*), nullptr,                   // NOLINT
            static_cast<yakushima::value_align_type>(sizeof(Record*)), // NOLINT
            &nvp)};
    if (insert_result == yakushima::status::OK) {
        Status check_node_set_res{ti->update_node_set(nvp)};
        if (check_node_set_res == Status::ERR_PHANTOM) {
            /**
              * This This transaction is confirmed to be aborted 
              * because the previous scan was destroyed by an insert
              * by another transaction.
              */
            batch::abort(ti);
            return Status::ERR_PHANTOM;
        }
        ti->get_write_set().push({storage, OP_TYPE::INSERT, rec_ptr});
        return Status::OK;
    }
    // else insert_result == Status::WARN_ALREADY_EXISTS
    // so retry from index access
    delete rec_ptr; // NOLINT
    return Status::WARN_CONCURRENT_INSERT;
}

Status upsert(session* ti, Storage storage, const std::string_view key,
              const std::string_view val) {
    // checks
    if (ti->get_mode() == tx_mode::BATCH &&
        epoch::get_global_epoch() < ti->get_valid_epoch()) {
        // not in valid epoch.
        return Status::WARN_PREMATURE;
    }
    if (ti->get_read_only()) {
        // can't write in read only mode.
        return Status::WARN_INVALID_HANDLE;
    }
    if (ti->get_mode() == tx_mode::BATCH && !ti->check_exist_wp_set(storage)) {
        // can't write without wp.
        return Status::WARN_INVALID_ARGS;
    }

RETRY_INDEX_ACCESS:

    // index access
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            key))};                                               // NOLINT
    Record* rec_ptr{};
    if (rec_d_ptr != nullptr) {
        rec_ptr = *rec_d_ptr;

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