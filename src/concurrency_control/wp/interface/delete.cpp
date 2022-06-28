
#include "atomic_wrapper.h"

#include "storage.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/local_set.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/include/helper.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"
#include "shirakami/logging.h"

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
    delete_tid.set_latest(false);
    delete_tid.set_lock(false);
    storeRelease(rec_ptr->get_tidw_ref().get_obj(), delete_tid.get_obj());
}

inline Status process_after_write(session* ti, write_set_obj* wso) {
    if (wso->get_op() == OP_TYPE::INSERT) {
        cancel_insert(wso->get_rec_ptr(), ti->get_step_epoch());
        ti->get_write_set().erase(wso);
        return Status::WARN_CANCEL_PREVIOUS_INSERT;
    }
    if (wso->get_op() == OP_TYPE::UPDATE) {
        ti->get_write_set().erase(wso);
        return Status::WARN_CANCEL_PREVIOUS_UPDATE;
    }
    if (wso->get_op() == OP_TYPE::DELETE) { return Status::OK; }
    LOG(ERROR) << "unknown code path";
    return Status::ERR_FATAL;
}

Status delete_record(Token token, Storage storage,
                     const std::string_view key) { // NOLINT
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();

    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    ti->process_before_start_step();

    // check for write
    auto rc{check_before_write_ops(ti, storage, OP_TYPE::DELETE)};
    if (rc != Status::OK) {
        ti->process_before_finish_step();
        return rc;
    }

    // index access to check local write set
    Record* rec_ptr{};
    rc = get<Record>(storage, key, rec_ptr);
    if (Status::OK == rc) {
        // check local write
        write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
        if (in_ws != nullptr) {
            ti->process_before_finish_step();
            return process_after_write(ti, in_ws);
        }

        // check absent
        tid_word ctid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        if (ctid.get_absent()) {
            ti->process_before_finish_step();
            return Status::WARN_NOT_FOUND;
        }

        // prepare write
        ti->get_write_set().push({storage, OP_TYPE::DELETE, rec_ptr}); // NOLINT
        ti->process_before_finish_step();
        return Status::OK;
    }
    if (rc == Status::WARN_NOT_FOUND) {
        ti->process_before_finish_step();
        return Status::WARN_NOT_FOUND;
    }
    if (rc == Status::WARN_STORAGE_NOT_FOUND) {
        ti->process_before_finish_step();
        return Status::WARN_STORAGE_NOT_FOUND;
    }
    LOG(ERROR) << "programming error: " << rc;
    ti->process_before_finish_step();
    return Status::ERR_FATAL;
}

} // namespace shirakami