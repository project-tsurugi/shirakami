
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "glog/logging.h"

namespace shirakami::occ {

Status abort(Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);
    ti->clean_up();
    return Status::OK;
}

void write_phase(Token token) {
    auto* ti = static_cast<session*>(token);

    auto process = [](write_set_obj* wso_ptr) {
        switch (wso_ptr->get_op()) {
            case OP_TYPE::INSERT: {
                tid_word update_tid{wso_ptr->get_rec_ptr()->get_tidw()};
                update_tid.set_lock(false);
                update_tid.set_absent(false);
                wso_ptr->get_rec_ptr()->set_tid(update_tid);
                break;
            }
            case OP_TYPE::UPDATE: {
                std::string* old_v{};
                wso_ptr->get_rec_ptr()->get_latest()->set_value(
                        wso_ptr->get_val(), old_v);

                tid_word update_tid{wso_ptr->get_rec_ptr()->get_tidw()};
                update_tid.set_lock(false);
                update_tid.set_absent(false);
                wso_ptr->get_rec_ptr()->set_tid(update_tid);
                break;
            }
            default: {
                LOG(FATAL) << "unknown operation type.";
                break;
            }
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


}

extern Status commit(Token token, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    auto* ti = static_cast<session*>(token);

    // write lock phase

    // wp verify

    // read verify

    // node verify

    // write phase
    write_phase(token);

    // clean up local set
    ti->clean_up();

    return Status::OK;
}

} // namespace shirakami::occ