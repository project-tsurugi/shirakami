
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/interface/include/helper.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"
#include "shirakami/logging.h"

namespace shirakami {

static void register_read_if_ltx(session* const ti, Record* const rec_ptr) {
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        ti->read_set_for_ltx().push(rec_ptr);
    }
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
            LOG(ERROR) << "unexpected error";
            return;
        }
        // get range read  by info
        range_read_by_long* rrbp{psm->get_range_read_by_long_ptr()};
        ti->get_range_read_by_long_set().insert(std::make_tuple(
                rrbp, std::string(key), scan_endpoint::INCLUSIVE,
                std::string(key), scan_endpoint::INCLUSIVE));
    }
}

Status update(Token token, Storage storage,
              const std::string_view key, // NOLINT
              const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin({token}); // NOLINT
    }
    ti->process_before_start_step();

    // check for write
    auto rc{check_before_write_ops(ti, storage, key, OP_TYPE::UPDATE)};
    if (rc != Status::OK) {
        ti->process_before_finish_step();
        return rc;
    }

    // index access to check local write set
    Record* rec_ptr{};
    if (Status::OK == get<Record>(storage, key, rec_ptr)) {
        // check local write
        write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
        if (in_ws != nullptr) {
            if (in_ws->get_op() == OP_TYPE::DELETE) {
                ti->process_before_finish_step();
                return Status::WARN_ALREADY_DELETE;
            }
            in_ws->set_val(val);
            ti->process_before_finish_step();
            return Status::OK;
        }

        // check absent
        tid_word ctid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        if (ctid.get_absent()) {
            process_before_return_not_found(ti, storage, key);
            ti->process_before_finish_step();
            return Status::WARN_NOT_FOUND;
        }

        // prepare write
        ti->get_write_set().push(
                {storage, OP_TYPE::UPDATE, rec_ptr, val}); // NOLINT
        register_read_if_ltx(ti, rec_ptr);
        ti->process_before_finish_step();
        return Status::OK;
    }
    process_before_return_not_found(ti, storage, key);
    ti->process_before_finish_step();
    return Status::WARN_NOT_FOUND;
}

} // namespace shirakami