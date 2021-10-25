
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status abort([[maybe_unused]] Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);
    ti->clean_up_local_set();
    ti->clean_up_tx_property();
    return Status::OK;
}

extern Status commit([[maybe_unused]] Token token, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    auto* ti = static_cast<session*>(token);

    auto process = [](write_set_obj* wso_ptr) {
        tid_word update_tid{wso_ptr->get_rec_ptr()->get_tidw()};
        update_tid.set_lock(false);
        update_tid.set_absent(false);
        switch (wso_ptr->get_op()) {
            case OP_TYPE::INSERT: {
                wso_ptr->get_rec_ptr()->set_tid(update_tid);
                break;
            }
            case OP_TYPE::UPDATE: {
                wso_ptr->get_rec_ptr()->set_tid(update_tid);
                break;
            }
            default:
                LOG(FATAL) << "unknown operation type.";
        }
    };

    if (ti->get_write_set().get_for_batch()) {
        for (auto&& elem : ti->get_write_set().get_ref_cont_for_bt()) {
            process(&std::get<1>(elem));
        }
    } else {
        for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
            process(&elem);
        }
    }


    // occ
    // write lock
    // epoch load
    // serialization point
    // wp verify
    // read verify
    // node verify

    // batch

    // clean up local set
    ti->clean_up_local_set();
    ti->clean_up_tx_property();
    return Status::OK;
}

extern bool check_commit([[maybe_unused]] Token token, // NOLINT
                         [[maybe_unused]] std::uint64_t commit_id) {
    // todo
    return true;
}

} // namespace shirakami
