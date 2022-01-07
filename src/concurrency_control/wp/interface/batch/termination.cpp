
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
        if (elem_ptr == nullptr) { LOG(FATAL); }
        if (Status::OK != (*elem_ptr)->remove_wp(ti->get_batch_id())) {
            LOG(FATAL);
        }
    }
}

void cleanup_process(session* const ti) {
    remove_wps(ti);
    ti->clean_up();
}

void cancel_flag_inserted_records(session* const ti) {
    auto process = [](std::pair<Record* const, write_set_obj>& wse) {
        auto&& wso = std::get<1>(wse);
        if (wso.get_op() == OP_TYPE::INSERT) {
            auto* rec_ptr = std::get<0>(wse);
            tid_word tid{0};
            tid.set_latest(true);
            tid.set_absent(true);
            rec_ptr->set_tid(tid);
        }
    };

    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        process(wso);
    }
}

Status abort(session* ti) { // NOLINT
    cancel_flag_inserted_records(ti);

    // clean up
    cleanup_process(ti);
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
                // unlock and set ctid
                rec_ptr->set_tid(ctid);
                break;
            }
            case OP_TYPE::UPSERT: {
                // lock record
                rec_ptr->get_tidw_ref().lock();
                tid_word pre_tid{rec_ptr->get_tidw_ref().get_obj()};

                if (ti->get_valid_epoch() > pre_tid.get_epoch()) {
                    // case: first of list
                    version* new_v{new version( // NOLINT
                            wso.get_val(), rec_ptr->get_latest())};
                    pre_tid.set_absent(false);
                    pre_tid.set_latest(false);
                    pre_tid.set_lock(false);
                    rec_ptr->get_latest()->set_tid(pre_tid);
                    rec_ptr->set_latest(new_v);
                    // unlock and set ctid
                    rec_ptr->set_tid(ctid);
                } else {
                    // case: middle of list
                    version* pre_ver{rec_ptr->get_latest()};
                    version* ver{rec_ptr->get_latest()->get_next()};
                    tid_word tid{ver->get_tid()};
                    for (;;) {
                        if (tid.get_epoch() < ti->get_valid_epoch()) {
                            version* new_v{
                                    new version(ctid, wso.get_val(), ver)};
                            pre_ver->set_next(new_v);
                            break;
                        }
                        if (tid.get_epoch() == ti->get_valid_epoch()) {
                            // para (partial order) write, invisible write
                            break;
                        } else {
                            pre_ver = ver;
                            ver = ver->get_next();
                            tid = ver->get_tid();
                        }
                    }
                    rec_ptr->get_tidw_ref().unlock();
                }
                break;
            }
            default: {
                LOG(FATAL) << "unknown operation type.";
                break;
            }
        }
    };

    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        process(wso);
    }
}

extern Status commit(session* ti, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    expose_local_write(ti);

    // todo enhancement
    /**
     * Sort by wp and then globalize the local write set. 
     * Eliminate wp from those that have been globalized in wp units.
     */


    // clean up
    cleanup_process(ti);
    return Status::OK;
}

} // namespace shirakami::batch