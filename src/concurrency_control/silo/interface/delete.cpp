/**
 * @file delete.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"
#include "storage.h"

#include "include/helper.h"

#include "shirakami/interface.h"
#include "shirakami/logging.h"

#include "index/yakushima/include/interface.h"

#include "glog/logging.h"

// sizeof(Tuple)

namespace shirakami {

[[maybe_unused]] Status delete_all_records() { // NOLINT
    std::vector<Storage> storage_list;
    list_storage(storage_list);
    for (auto&& elem : storage_list) { storage::delete_storage(elem); }

    yakushima::destroy();

    return Status::OK;
}

Status delete_record(Token token, Storage storage,
                     const std::string_view key) { // NOLINT
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) {
        VLOG(log_warning) << "delete on read only transaction";
        return Status::WARN_ILLEGAL_OPERATION;
    }

    Record* rec_ptr{};
    auto rc{get<Record>(storage, key, rec_ptr)};
    if (rc != Status::OK) { return rc; }

    Status check = ti->check_delete_after_write(rec_ptr);
    if (check == Status::WARN_CANCEL_PREVIOUS_INSERT ||
        check == Status::WARN_CANCEL_PREVIOUS_UPDATE) {
        return check;
    }
    if (check == Status::WARN_ALREADY_DELETE) {
        /**
         * From the user's point of view, this operation does not change the 
         * some effect.
         */
        return Status::OK;
    }

    tid_word check_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
    if (check_tid.get_absent()) {
        /**
         * The second condition checks
         * whether the record you want to read should not be read by parallel
         * insert / delete.
         */
        return Status::WARN_NOT_FOUND;
    }

    ti->get_write_set().push({storage, OP_TYPE::DELETE, rec_ptr});
    return check;
}

} // namespace shirakami
