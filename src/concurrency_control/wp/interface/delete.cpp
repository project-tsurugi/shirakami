
#include "atomic_wrapper.h"

#include "storage.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/local_set.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/include/helper.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

namespace shirakami {

[[maybe_unused]] Status delete_all_records() { // NOLINT
    std::vector<Storage> storage_list;
    list_storage(storage_list);
    for (auto&& elem : storage_list) {
        if (elem != wp::get_page_set_meta_storage()) {
            storage::delete_storage(elem);
        }
    }

    return Status::OK;
}

inline void cancel_insert(Record* rec_ptr, epoch::epoch_t e) {
    tid_word delete_tid{};
    delete_tid.set_epoch(e);
    delete_tid.set_absent(true);
    delete_tid.set_latest(true);
    delete_tid.set_lock(false);
    storeRelease(rec_ptr->get_tidw_ref().get_obj(), delete_tid.get_obj());
}

inline void process_after_write(session* ti, write_set_obj* wso) {
    if (wso->get_op() == OP_TYPE::INSERT) {
        cancel_insert(wso->get_rec_ptr(), ti->get_step_epoch());
    } else if (wso->get_op() == OP_TYPE::UPDATE) {
        ti->get_write_set().erase(wso);
    }
}

Status delete_record(Token token, Storage storage,
                     const std::string_view key) { // NOLINT
    auto* ti = static_cast<session*>(token);

    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    // check for write
    auto rc{check_before_write_ops(ti, storage)};
    if (rc != Status::OK) { return rc; }

    //update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    // index access to check local write set
    Record* rec_ptr{};
    if (Status::OK == get<Record>(storage, key, rec_ptr)) {
        // check local write
        write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
        if (in_ws != nullptr) {
            process_after_write(ti, in_ws);
            return Status::WARN_WRITE_TO_LOCAL_WRITE;
        }

        // prepare write
        ti->get_write_set().push({storage, OP_TYPE::DELETE, rec_ptr}); // NOLINT
        return Status::OK;
    } else {
        return Status::WARN_NOT_FOUND;
    }
}

} // namespace shirakami