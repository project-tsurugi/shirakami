
#include <string_view>

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/wp/interface/batch/include/batch.h"

#include "glog/logging.h"

namespace shirakami::batch {

void remove_wps(session* ti) {
    for (auto&& storage : ti->get_wp_set()) {
        Storage page_set_meta_storage = wp::get_page_set_meta_storage();
        std::string_view page_set_meta_storage_view = {
                reinterpret_cast<char*>( // NOLINT
                        &page_set_meta_storage),
                sizeof(page_set_meta_storage)};
        std::string_view storage_view = {
                reinterpret_cast<char*>(&storage), // NOLINT
                sizeof(storage)};
        auto* elem_ptr = std::get<0>(yakushima::get<wp::wp_meta*>(
                page_set_meta_storage_view, storage_view));
        if (elem_ptr == nullptr) {
            LOG(FATAL);
            std::abort();
        }
        if (Status::OK != (*elem_ptr)->remove_wp(ti->get_batch_id())) {
            LOG(FATAL);
            std::abort();
        }
    }
}

Status abort(session* ti) { // NOLINT
    // clean up
    remove_wps(ti);
    ti->clean_up();
    return Status::OK;
}

void compute_tid(session* ti, tid_word& ctid) {
    ctid.set_epoch(ti->get_valid_epoch());
    ctid.set_lock(false);
    ctid.set_absent(false);
    ctid.set_latest(true);
}

void expose_local_write(session* ti) {
    auto process = [ti](std::pair<Record* const, write_set_obj>& wse) {
        auto* rec_ptr = std::get<0>(wse);
        auto&& wso = std::get<1>(wse);
        tid_word ctid{};
        compute_tid(ti, ctid);
        switch (wso.get_op()) {
            case OP_TYPE::INSERT: {
                // todo wp-k
                break;
            }
            case OP_TYPE::UPDATE: {
                version* new_v{new version(rec_ptr->get_tidw(), wso.get_val(), // NOLINT
                                           rec_ptr->get_latest())};
                rec_ptr->set_latest(new_v);
                break;
            }
            default: {
                LOG(FATAL) << "unknown operation type.";
                break;
            }
        }
        rec_ptr->set_tid(ctid);
    };

    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        process(wso);
    }
}

extern Status commit(session* ti, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    expose_local_write(ti);

    // clean up
    // todo enhancement
    /**
     * Sort by wp and then globalize the local write set. 
     * Eliminate wp from those that have been globalized in wp units.
     */
    remove_wps(ti);

    ti->clean_up();
    return Status::OK;
}

} // namespace shirakami::batch