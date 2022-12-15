
#include "atomic_wrapper.h"

#include "storage.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/local_set.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/include/helper.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"
#include "shirakami/logging.h"

namespace shirakami {

[[maybe_unused]] Status delete_all_records() { // NOLINT
    //check list of all storage
    std::vector<Storage> storage_list;
    storage::list_storage(storage_list);

    // delete all storages.
    for (auto&& elem : storage_list) {
        if (elem != wp::get_page_set_meta_storage()) {
            if (delete_storage(elem) != Status::OK) {
                LOG(ERROR) << log_location_prefix << "try delete_storage("
                           << elem << ")";
                return Status::ERR_FATAL;
            }
        }
    }

    storage::key_handle_map_clear();

    return Status::OK;
}

inline void cancel_insert_if_tomb_stone(Record* rec_ptr, epoch::epoch_t e) {
    rec_ptr->get_tidw_ref().lock();
    tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
    if (check.get_absent() && check.get_latest()) {
        tid_word delete_tid{};
        delete_tid.set_epoch(e);
        delete_tid.set_absent(true);
        delete_tid.set_latest(false);
        delete_tid.set_lock(false);
        storeRelease(rec_ptr->get_tidw_ref().get_obj(), delete_tid.get_obj());
    }
    rec_ptr->get_tidw_ref().unlock();
}

static void register_read_if_ltx(session* const ti, Record* const rec_ptr) {
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        ti->read_set_for_ltx().push(rec_ptr);
    }
}

inline Status process_after_write(session* ti, write_set_obj* wso) {
    if (wso->get_op() == OP_TYPE::INSERT) {
        cancel_insert_if_tomb_stone(wso->get_rec_ptr(), ti->get_step_epoch());
        ti->get_write_set().erase(wso);
        // insert operation already registered read for ltx
        return Status::WARN_CANCEL_PREVIOUS_INSERT;
    }
    if (wso->get_op() == OP_TYPE::UPDATE) {
        wso->set_op(OP_TYPE::DELETE);
        // update operation already registered read for ltx
        return Status::OK;
    }
    if (wso->get_op() == OP_TYPE::DELETE) {
        // delete operation already registered read for ltx
        return Status::OK;
    }
    if (wso->get_op() == OP_TYPE::UPSERT) {
        cancel_insert_if_tomb_stone(wso->get_rec_ptr(), ti->get_step_epoch());
        ti->get_write_set().erase(wso);
        return Status::WARN_CANCEL_PREVIOUS_UPSERT;
    }
    LOG(ERROR) << log_location_prefix << "unknown code path";
    return Status::ERR_FATAL;
}

static void process_before_return_not_found(session* const ti,
                                            Storage const storage,
                                            std::string_view const key) {
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        /**
             * Normally, read information is stored at page, but the page is not
             * found. So it stores at table level information as range, 
             * key <= range <= key.
             */
        // get page set meta info
        wp::page_set_meta* psm{};
        auto rc = wp::find_page_set_meta(storage, psm);
        if (rc != Status::OK) {
            LOG(ERROR) << log_location_prefix << "unexpected error";
            return;
        }
        // get range read  by info
        range_read_by_long* rrbp{psm->get_range_read_by_long_ptr()};
        ti->get_range_read_by_long_set().insert(std::make_tuple(
                rrbp, std::string(key), scan_endpoint::INCLUSIVE,
                std::string(key), scan_endpoint::INCLUSIVE));
    }
}

Status delete_record(Token token, Storage storage,
                     const std::string_view key) { // NOLINT
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();

    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin({token}); // NOLINT
    }
    ti->process_before_start_step();

    // check for write
    auto rc{check_before_write_ops(ti, storage, key, OP_TYPE::DELETE)};
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
            process_before_return_not_found(ti, storage, key);
            return Status::WARN_NOT_FOUND;
        }

        // prepare write
        ti->get_write_set().push({storage, OP_TYPE::DELETE, rec_ptr}); // NOLINT
        register_read_if_ltx(ti, rec_ptr);
        ti->process_before_finish_step();
        return Status::OK;
    }
    if (rc == Status::WARN_NOT_FOUND) {
        process_before_return_not_found(ti, storage, key);
        ti->process_before_finish_step();
        return Status::WARN_NOT_FOUND;
    }
    if (rc == Status::WARN_STORAGE_NOT_FOUND) {
        ti->process_before_finish_step();
        return Status::WARN_STORAGE_NOT_FOUND;
    }
    LOG(ERROR) << log_location_prefix << "unexpected error: " << rc;
    ti->process_before_finish_step();
    return Status::ERR_FATAL;
}

} // namespace shirakami