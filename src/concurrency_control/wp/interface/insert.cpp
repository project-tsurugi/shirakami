
#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/interface/include/helper.h"

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
        ti->get_write_set().push({st, OP_TYPE::INSERT, rec_ptr, val});
        return Status::OK;
    }
    // else insert_result == Status::WARN_ALREADY_EXISTS
    // so retry from index access
    delete rec_ptr; // NOLINT
    return Status::WARN_CONCURRENT_INSERT;
}

Status insert(Token const token, Storage const storage,
              const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    if (!ti->get_tx_began()) {
        tx_begin({token}); // NOLINT
    }
    ti->process_before_start_step();

    // check for write
    auto rc{check_before_write_ops(ti, storage, OP_TYPE::INSERT)};
    if (rc != Status::OK) {
        ti->process_before_finish_step();
        return rc;
    }

    for (;;) {
        // index access to check local write set
        Record* rec_ptr{};
        if (Status::OK == get<Record>(storage, key, rec_ptr)) {
            write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)};
            if (in_ws != nullptr) {
                if (in_ws->get_op() == OP_TYPE::INSERT ||
                    in_ws->get_op() == OP_TYPE::UPDATE ||
                    in_ws->get_op() == OP_TYPE::UPSERT) {
                    ti->process_before_finish_step();
                    return Status::WARN_ALREADY_EXISTS;
                }
                if (in_ws->get_op() == OP_TYPE::DELETE) {
                    in_ws->set_op(OP_TYPE::UPDATE);
                    in_ws->set_val(val);
                    ti->process_before_finish_step();
                    return Status::OK;
                }
            }

            tid_word found_tid{};
            rc = try_deleted_to_inserting(ti->get_tx_type(), rec_ptr,
                                          found_tid);
            if (rc == Status::OK) {
                ti->get_write_set().push(
                        {storage, OP_TYPE::INSERT, rec_ptr, val});
                ti->process_before_finish_step();
                return Status::OK;
            }
            if (rc == Status::WARN_ALREADY_EXISTS) {
                // ==========
                // start: make read set
                if (ti->get_tx_type() ==
                    transaction_options::transaction_type::SHORT) {
                    ti->get_read_set().emplace_back(storage, rec_ptr,
                                                    found_tid);
                } else if (ti->get_tx_type() ==
                           transaction_options::transaction_type::LONG) {
                    // register read_by_set
                    point_read_by_long* rbp{};
                    auto rc = wp::find_read_by(storage, rbp);
                    if (rc == Status::OK) {
                        ti->get_point_read_by_long_set().insert(rbp);
                    } else {
                        LOG(ERROR) << "programming error";
                        ti->process_before_finish_step();
                        return Status::ERR_FATAL;
                    }
                } else {
                    LOG(ERROR) << "programming error";
                    ti->process_before_finish_step();
                    return Status::ERR_FATAL;
                }
                // end: make read set
                // ==========
            } else if (rc == Status::WARN_CONCURRENT_INSERT) {
                ti->get_write_set().push(
                        {storage, OP_TYPE::INSERT, rec_ptr, val});
                ti->process_before_finish_step();
                return Status::OK;
            }
            ti->process_before_finish_step();
            return rc;
        }

        auto rc{insert_process(ti, storage, key, val)};
        if (rc == Status::OK) {
            ti->process_before_finish_step();
            return rc;
        }
        if (rc == Status::ERR_PHANTOM) { return rc; }
    }
}

} // namespace shirakami