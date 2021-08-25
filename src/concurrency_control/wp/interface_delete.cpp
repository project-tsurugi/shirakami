/**
 * @file interface_delete.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"
#include "storage.h"

#include "include/interface_helper.h"

#include "shirakami/interface.h"

// sizeof(Tuple)

namespace shirakami {

[[maybe_unused]] Status delete_all_records() { // NOLINT
    std::vector<Storage> storage_list;
    list_storage(storage_list);
    for (auto&& elem : storage_list) {
        storage::delete_storage(elem);
    }

    yakushima::destroy();

    return Status::OK;
}

Status delete_record(Token token, Storage storage, const std::string_view key) { // NOLINT
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) return Status::WARN_INVALID_HANDLE;

    Record** rec_double_ptr{yakushima::get<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, key).first}; // NOLINT
    if (rec_double_ptr == nullptr) {
        return Status::WARN_NOT_FOUND;
    }

    Record* rec_ptr{*rec_double_ptr};
    Status check = ti->check_delete_after_write(rec_ptr);
    if (check == Status::WARN_CANCEL_PREVIOUS_INSERT) {
        return check;
    }

    tid_word check_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
    if (check_tid.get_absent()) {
        // The second condition checks
        // whether the record you want to read should not be read by parallel
        // insert / delete.
        return Status::WARN_NOT_FOUND;
    }

    ti->get_write_set().push({storage, OP_TYPE::DELETE, rec_ptr});
    return check;
}

} // namespace shirakami
