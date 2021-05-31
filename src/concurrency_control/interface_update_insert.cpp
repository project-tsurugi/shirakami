/**
 * @file interface_update_insert.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"

#include "concurrency_control/include/interface_helper.h"

#include "shirakami/interface.h"

#include "tuple_local.h" // sizeof(Tuple)

namespace shirakami {

Status insert(Token token, Storage storage, const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) return Status::WARN_INVALID_HANDLE;

    Record** existing_rec_ptr{std::get<0>(yakushima::get<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, key))}; // NOLINT
    if (existing_rec_ptr != nullptr) {
        write_set_obj* inws{ti->get_write_set().search(*existing_rec_ptr)}; // NOLINT
        if (inws != nullptr) {
            if (inws->get_op() == OP_TYPE::INSERT || inws->get_op() == OP_TYPE::UPDATE) {
                inws->reset_tuple_value(val);
            } else if (inws->get_op() == OP_TYPE::DELETE) {
                *inws = write_set_obj{storage, key, val, OP_TYPE::UPDATE, inws->get_rec_ptr()};
            }
            return Status::WARN_WRITE_TO_LOCAL_WRITE;
        }

        return Status::WARN_ALREADY_EXISTS;
    }

    Record* rec_ptr = new Record(key, val); // NOLINT
    yakushima::node_version64* nvp{};
    yakushima::status insert_result{
            yakushima::put<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, key, &rec_ptr, sizeof(Record*), nullptr, // NOLINT
                                    static_cast<yakushima::value_align_type>(sizeof(Record*)), &nvp)};                             // NOLINT
    if (insert_result == yakushima::status::OK) {
        ti->get_write_set().push({storage, OP_TYPE::INSERT, rec_ptr});
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
    delete rec_ptr; // NOLINT
    return Status::WARN_ALREADY_EXISTS;
}

Status update(Token token, Storage storage, const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) return Status::WARN_INVALID_HANDLE;

    Record** existing_rec_ptr{
            std::get<0>(yakushima::get<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, key))}; // NOLINT
    if (existing_rec_ptr == nullptr) {
        return Status::WARN_NOT_FOUND;
    }
    write_set_obj* inws{ti->get_write_set().search(*existing_rec_ptr)}; // NOLINT
    if (inws != nullptr) {
        if (inws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        inws->reset_tuple_value(val);
        return Status::WARN_WRITE_TO_LOCAL_WRITE;
    }

    Record* rec_ptr{*existing_rec_ptr};
    tid_word check_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
    if (check_tid.get_absent()) {
        // The second condition checks
        // whether the record you want to read should not be read by parallel
        // insert / delete.
        return Status::WARN_NOT_FOUND;
    }

    ti->get_write_set().push({storage, key, val, OP_TYPE::UPDATE, rec_ptr});

    return Status::OK;
}

Status upsert(Token token, Storage storage, const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) return Status::WARN_INVALID_HANDLE;

RETRY_FIND_RECORD:
    Record** existing_rec_ptr{
            std::get<0>(yakushima::get<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, key))}; // NOLINT
    Record* rec_ptr{};
    if (existing_rec_ptr != nullptr) {
        rec_ptr = *existing_rec_ptr;
        write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
        if (in_ws != nullptr) {
            if (in_ws->get_op() == OP_TYPE::INSERT || in_ws->get_op() == OP_TYPE::UPDATE) {
                in_ws->reset_tuple_value(val);
            } else if (in_ws->get_op() == OP_TYPE::DELETE) {
                *in_ws = write_set_obj{storage, key, val, OP_TYPE::UPDATE, in_ws->get_rec_ptr()};
            }
            return Status::WARN_WRITE_TO_LOCAL_WRITE;
        }
        // do update
    } else {
        rec_ptr = nullptr;
        // do insert
    }

    if (rec_ptr == nullptr) {
        rec_ptr = new Record(key, val); // NOLINT
        yakushima::node_version64* nvp{};
        yakushima::status insert_result{
                yakushima::put<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, key, &rec_ptr, sizeof(Record*), nullptr, // NOLINT
                                        static_cast<yakushima::value_align_type>(sizeof(Record*)), &nvp)};                             // NOLINT
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
            ti->get_write_set().push({storage, OP_TYPE::INSERT, rec_ptr});
            return Status::OK;
        }
        // else insert_result == Status::WARN_ALREADY_EXISTS
        // so goto update.
        delete rec_ptr;         // NOLINT
        goto RETRY_FIND_RECORD; // NOLINT
    }

    tid_word check_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
    if (check_tid.get_latest() && check_tid.get_absent()) {
        /**
         * The record being inserted has been detected.
         */
        return Status::WARN_CONCURRENT_INSERT;
    }
    if (!check_tid.get_latest() && check_tid.get_absent()) {
        /**
         * It was detected between the logical deletion operation and the physical deletion operation (unhook operation).
         */
        return Status::WARN_CONCURRENT_DELETE;
    }

    ti->get_write_set().push({storage, key, val, OP_TYPE::UPDATE, rec_ptr}); // NOLINT

    return Status::OK;
} // namespace shirakami

} // namespace shirakami
