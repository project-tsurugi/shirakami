
#include "atomic_wrapper.h"

#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "glog/logging.h"

namespace shirakami::occ {

void unlock_write_set(session* ti) {
    for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
        itr.get_rec_ptr()->get_tidw_ref().unlock();
    }
}

void unlock_not_insert_records(session* ti, std::size_t not_insert_locked_num);

Status abort(session* ti) { // NOLINT
    ti->clean_up();
    return Status::OK;
}

Status read_verify(session* ti) {
    tid_word check{};
    for (auto&& itr : ti->get_read_set()) {
        auto* rec_ptr = itr.get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw_ref().get_obj());
        if (itr.get_tid().get_tid() != check.get_tid() ||
        check.get_absent() ||
        (check.get_lock() && ti->get_write_set().search(rec_ptr) == nullptr)) {
            unlock_write_set(ti);
            occ::abort(ti);
            return Status::ERR_VALIDATION;
        }
    }

    return Status::OK;
}

void unlock_not_insert_records(session* ti, std::size_t not_insert_locked_num) {
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        if (not_insert_locked_num == 0) { break; }
        auto* wso_ptr = &(elem);
        if (wso_ptr->get_op() == OP_TYPE::INSERT) { continue; }
        wso_ptr->get_rec_ptr()->get_tidw_ref().unlock();
        --not_insert_locked_num;
    }
}

Status wp_verify(session* ti) {
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        auto wps = find_wp(elem.get_storage());
        if (!wps.empty()) { return Status::ERR_FAIL_WP; }
    }
    return Status::OK;
}

Status write_lock(session* ti) {
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
                    unlock_not_insert_records(ti, not_insert_locked_num);
                }
                occ::abort(ti);
                return Status::ERR_WRITE_TO_DELETED_RECORD;
            }
        }
    }

    return Status::OK;
}

void write_phase(session* ti) {
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

extern Status commit(session* ti, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    // write lock phase
    auto rc{write_lock(ti)};
    if (rc != Status::OK) { return rc; }

    // wp verify
    rc = wp_verify(ti);
    if (rc != Status::OK) { return rc; }

    // read verify
    rc = read_verify(ti);
    if (rc != Status::OK) { return rc; }

    // node verify

    // write phase
    write_phase(ti);

    // clean up local set
    ti->clean_up();

    return Status::OK;
}

} // namespace shirakami::occ