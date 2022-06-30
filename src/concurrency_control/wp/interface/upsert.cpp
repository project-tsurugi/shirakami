

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/interface/include/helper.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"
#include "concurrency_control/wp/interface/short_tx/include/short_tx.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

static inline Status insert_process(session* const ti, Storage st,
                             const std::string_view key,
                             const std::string_view val) {
    Record* rec_ptr{};
    rec_ptr = new Record(key); // NOLINT
    yakushima::node_version64* nvp{};
    // create tombstone
    if (yakushima::status::OK ==
        put<Record>(ti->get_yakushima_token(), st, key, rec_ptr, nvp)) {
        Status check_node_set_res{ti->update_node_set(nvp)};
        if (check_node_set_res == Status::ERR_PHANTOM) {
            /**
                         * This This transaction is confirmed to be aborted 
                         * because the previous scan was destroyed by an insert
                         * by another transaction.
                         */
            abort(ti);
            return Status::ERR_PHANTOM;
        }
        ti->get_write_set().push({st, OP_TYPE::UPSERT, rec_ptr, val});
        return Status::OK;
    }
    // fail insert rec_ptr
    delete rec_ptr; // NOLINT
    return Status::WARN_CONCURRENT_INSERT;
}

Status upsert(Token token, Storage storage, const std::string_view key,
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    ti->process_before_start_step();

    // check for write
    auto rc{check_before_write_ops(ti, storage, OP_TYPE::UPSERT)};
    if (rc != Status::OK) {
        ti->process_before_finish_step();
        return rc;
    }

    for (;;) {
        // index access to check local write set
        Record* rec_ptr{};
        if (Status::OK == get<Record>(storage, key, rec_ptr)) {
            // check local write
            write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
            if (in_ws != nullptr) {
                if (in_ws->get_op() == OP_TYPE::DELETE) {
                    in_ws->set_op(OP_TYPE::UPDATE);
                    in_ws->set_val(val);
                } else {
                    in_ws->set_val(val);
                }
                ti->process_before_finish_step();
                return Status::WARN_WRITE_TO_LOCAL_WRITE;
            }

            // prepare update
            ti->get_write_set().push(
                    {storage, OP_TYPE::UPSERT, rec_ptr, val}); // NOLINT
            ti->process_before_finish_step();
            return Status::OK;
        }

        rc = insert_process(ti, storage, key, val);
        if (rc != Status::WARN_CONCURRENT_INSERT) {
            ti->process_before_finish_step();
            return rc;
        }
    }
}

} // namespace shirakami
