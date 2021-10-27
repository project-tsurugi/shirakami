
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

void unlock_not_insert_records(Token token, std::size_t not_insert_locked_num) {
    auto* ti = static_cast<session*>(token);
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        if (not_insert_locked_num == 0) { break; }
        auto* wso_ptr = &(elem);
        if (wso_ptr->get_op() == OP_TYPE::INSERT) { continue; }
        wso_ptr->get_rec_ptr()->get_tidw_ref().unlock();
        --not_insert_locked_num;
    }
}

Status write_lock(Token token) {
    auto* ti = static_cast<session*>(token);

    std::size_t not_insert_locked_num{0};
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        auto* wso_ptr = &(elem);
        if (wso_ptr->get_op() != OP_TYPE::INSERT) {
            auto* rec_ptr{wso_ptr->get_rec_ptr()};
            rec_ptr->get_tidw_ref().lock();
            ++not_insert_locked_num;
            if ((wso_ptr->get_op() == OP_TYPE::UPDATE ||
                 wso_ptr->get_op() == OP_TYPE::DELETE) &&
                rec_ptr->get_tidw_ref().get_absent()) {
                if (not_insert_locked_num > 0) {
                    unlock_not_insert_records(token, not_insert_locked_num);
                }
                occ::abort(token);
                return Status::ERR_WRITE_TO_DELETED_RECORD;
            }
        }
    }

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
    auto rc{write_lock(token)};
    if (rc != Status::OK) { return rc; }

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