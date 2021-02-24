/**
 * @file interface_update_insert.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"

#include "concurrency_control/silo_variant/include/garbage_collection.h"
#include "concurrency_control/silo_variant/include/interface_helper.h"

#include "kvs/interface.h"

#include "tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant {

Status insert(Token token, const std::string_view key,  // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) return Status::WARN_INVALID_HANDLE;

    write_set_obj* inws{ti->search_write_set(key)};
    if (inws != nullptr) {
        if (inws->get_op() == OP_TYPE::INSERT || inws->get_op() == OP_TYPE::UPDATE) {
            inws->reset_tuple_value(val);
        } else if (inws->get_op() == OP_TYPE::DELETE) {
            *inws = write_set_obj{key, val, OP_TYPE::UPDATE, inws->get_rec_ptr()};
        }
        return Status::WARN_WRITE_TO_LOCAL_WRITE;
    }

    if (std::get<0>(yakushima::get<Record*>(key)) != nullptr) {
        return Status::WARN_ALREADY_EXISTS;
    }

    Record* rec_ptr = new Record(key, val);  // NOLINT
    yakushima::node_version64* nvp{};
    yakushima::status insert_result{
            yakushima::put<Record*>(key, &rec_ptr, sizeof(Record*), nullptr, // NOLINT
                                    static_cast<yakushima::value_align_type>(sizeof(Record*)), &nvp)}; // NOLINT
    if (insert_result == yakushima::status::OK) {
        ti->get_write_set().emplace_back(OP_TYPE::INSERT, rec_ptr);
        Status check_node_set_res{ti->update_node_set(nvp)};
        if (check_node_set_res == Status::ERR_PHANTOM) {
            /**
             * This This transaction is confirmed to be aborted because the previous scan was destroyed by an insert
             * by another transaction.
             */
             abort(token);
            return Status::ERR_PHANTOM;
        }
        return Status::OK;
    }
    delete rec_ptr;  // NOLINT
    return Status::WARN_ALREADY_EXISTS;
}

Status update(Token token, const std::string_view key,  // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) return Status::WARN_INVALID_HANDLE;

    write_set_obj* inws{ti->search_write_set(key)};
    if (inws != nullptr) {
        inws->reset_tuple_value(val);
        return Status::WARN_WRITE_TO_LOCAL_WRITE;
    }

    Record** rec_double_ptr{
            std::get<0>(yakushima::get<Record*>(key))};
    if (rec_double_ptr == nullptr) {
        return Status::WARN_NOT_FOUND;
    }
    Record* rec_ptr{*rec_double_ptr};
    tid_word check_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
    if (check_tid.get_absent()) {
        // The second condition checks
        // whether the record you want to read should not be read by parallel
        // insert / delete.
        return Status::WARN_NOT_FOUND;
    }

    ti->get_write_set().emplace_back(key, val, OP_TYPE::UPDATE, rec_ptr);

    return Status::OK;
}

Status upsert(Token token, const std::string_view key,  // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) return Status::WARN_INVALID_HANDLE;
    write_set_obj* in_ws{ti->search_write_set(key)};
    if (in_ws != nullptr) {
        if (in_ws->get_op() == OP_TYPE::INSERT || in_ws->get_op() == OP_TYPE::UPDATE) {
            in_ws->reset_tuple_value(val);
        } else if (in_ws->get_op() == OP_TYPE::DELETE) {
            *in_ws = write_set_obj{key, val, OP_TYPE::UPDATE, in_ws->get_rec_ptr()};
        }
        return Status::WARN_WRITE_TO_LOCAL_WRITE;
    }

RETRY_FIND_RECORD:
    Record** rec_double_ptr{
            std::get<0>(yakushima::get<Record*>(key))};
    Record* rec_ptr{};
    if (rec_double_ptr == nullptr) {
        rec_ptr = nullptr;
    } else {
        rec_ptr = (*std::get<0>(yakushima::get<Record*>(key)));
    }
    if (rec_ptr == nullptr) {
        rec_ptr = new Record(key, val);  // NOLINT
        yakushima::node_version64* nvp{};
        yakushima::status insert_result{
                yakushima::put<Record*>(key, &rec_ptr, sizeof(Record*), nullptr, // NOLINT
                                        static_cast<yakushima::value_align_type>(sizeof(Record*)), &nvp)}; // NOLINT
        if (insert_result == yakushima::status::OK) {
            Status check_node_set_res{ti->update_node_set(nvp)};
            if (check_node_set_res == Status::ERR_PHANTOM) {
                /**
                 * This This transaction is confirmed to be aborted because the previous scan was destroyed by an insert
                 * by another transaction.
                 */
                 abort(token);
                return Status::ERR_PHANTOM;
            }
            ti->get_write_set().emplace_back(OP_TYPE::INSERT, rec_ptr);
            return Status::OK;
        }
        // else insert_result == Status::WARN_ALREADY_EXISTS
        // so goto update.
        delete rec_ptr;          // NOLINT
        goto RETRY_FIND_RECORD;  // NOLINT
    }
    ti->get_write_set().emplace_back(key, val, OP_TYPE::UPDATE,
                                     rec_ptr);  // NOLINT

    return Status::OK;
}  // namespace shirakami::silo_variant

}  // namespace shirakami::cc_silo_variant
