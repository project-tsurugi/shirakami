
#include "atomic_wrapper.h"

#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

#include "glog/logging.h"

namespace shirakami::occ {

void unlock_write_set(session* ti) {
    for (auto&& itr : ti->get_write_set().get_ref_cont_for_occ()) {
        itr.get_rec_ptr()->get_tidw_ref().unlock();
    }
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

Status abort(session* ti) { // NOLINT
    ti->clean_up();
    return Status::OK;
}

Status read_verify(session* ti, tid_word& commit_tid) {
    tid_word check{};
    for (auto&& itr : ti->get_read_set()) {
        const auto* rec_ptr = itr.get_rec_ptr();
        check.get_obj() = loadAcquire(rec_ptr->get_tidw_ref().get_obj());
        if (itr.get_tid().get_tid() != check.get_tid() ||
            itr.get_tid().get_epoch() != check.get_epoch() ||
            check.get_absent() ||
            (check.get_lock() &&
             ti->get_write_set().search(rec_ptr) == nullptr)) {
            unlock_write_set(ti);
            occ::abort(ti);
            return Status::ERR_VALIDATION;
        }
        commit_tid = std::max(check, commit_tid);
    }

    return Status::OK;
}

Status wp_verify(session* ti) {
    // sort and unique
    auto& st = ti->get_storage_set();
    std::sort(st.begin(), st.end());
    st.erase(std::unique(st.begin(), st.end()), st.end());

    for (auto&& elem : st) {
        auto wps = wp::find_wp(elem);
        if (!wp::wp_meta::empty(wps)) { return Status::ERR_FAIL_WP; }
    }

    return Status::OK;
}

Status write_lock(session* ti, tid_word& commit_tid) {
    std::size_t not_insert_locked_num{0};
    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        auto* wso_ptr = &(elem);
        if (wso_ptr->get_op() != OP_TYPE::INSERT) {
            auto* rec_ptr{wso_ptr->get_rec_ptr()};
            rec_ptr->get_tidw_ref().lock();
            commit_tid = std::max(commit_tid, rec_ptr->get_tidw_ref());
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

void write_phase(session* ti, epoch::epoch_t ce) {
    auto process = [ti, ce](write_set_obj* wso_ptr) {
        switch (wso_ptr->get_op()) {
            case OP_TYPE::INSERT: {
                wso_ptr->get_rec_ptr()->set_tid(ti->get_mrc_tid());
                break;
            }
            case OP_TYPE::UPDATE:
            case OP_TYPE::UPSERT: {
                tid_word old_tid{wso_ptr->get_rec_ptr()->get_tidw_ref()};
                if (ce != old_tid.get_epoch()) {
                    // append new version
                    Record* rec_ptr{wso_ptr->get_rec_ptr()};
                    version* new_v{new version(ti->get_mrc_tid(),
                                               wso_ptr->get_val(),
                                               rec_ptr->get_latest())};
                    tid_word old_version_tid{old_tid};
                    old_version_tid.set_latest(false);
                    old_version_tid.set_lock(false);
                    rec_ptr->get_latest()->set_tid(old_version_tid);
                    rec_ptr->set_latest(new_v);
                } else {
                    // update existing version
                    wso_ptr->get_rec_ptr()->get_latest()->set_value(
                            wso_ptr->get_val());
                }
                wso_ptr->get_rec_ptr()->set_tid(ti->get_mrc_tid());
                break;
            }
            default: {
                LOG(FATAL) << "unknown operation type.";
                break;
            }
        }
    };

    for (auto&& elem : ti->get_write_set().get_ref_cont_for_occ()) {
        process(&elem);
    }
}

Status node_verify(session* ti) {
    for (auto&& itr : ti->get_node_set()) {
        if (std::get<0>(itr) != std::get<1>(itr)->get_stable_version()) {
            unlock_write_set(ti);
            occ::abort(ti);
            return Status::ERR_PHANTOM;
        }
    }
    return Status::OK;
}

void compute_commit_tid(session* ti, epoch::epoch_t ce, tid_word& commit_tid) {
    tid_word tid_a{commit_tid};
    tid_a.inc_tid();

    tid_word tid_b{ti->get_mrc_tid()};
    tid_b.inc_tid();

    tid_word tid_c{};
    tid_c.set_epoch(ce);

    commit_tid = std::max({tid_a, tid_b, tid_c});
    commit_tid.set_lock(false);
    commit_tid.set_absent(false);
    commit_tid.set_latest(true);
    ti->set_mrc_tid(commit_tid);
}

extern Status commit(session* ti, // NOLINT
                     [[maybe_unused]] commit_param* cp) {
    // write lock phase
    tid_word commit_tid{};
    auto rc{write_lock(ti, commit_tid)};
    if (rc != Status::OK) {
        occ::abort(ti);
        return rc;
    }

    epoch::epoch_t ce{epoch::get_global_epoch()};

    // wp verify
    rc = wp_verify(ti);
    if (rc != Status::OK) {
        unlock_write_set(ti);
        occ::abort(ti);
        return rc;
    }

    // read verify
    rc = read_verify(ti, commit_tid);
    if (rc != Status::OK) {
        unlock_write_set(ti);
        occ::abort(ti);
        return rc;
    }

    // node verify
    rc = node_verify(ti);
    if (rc != Status::OK) {
        unlock_write_set(ti);
        occ::abort(ti);
        return rc;
    }

    compute_commit_tid(ti, ce, commit_tid);

    // write phase
    write_phase(ti, ce);

    // clean up local set
    ti->clean_up();

    return Status::OK;
}

} // namespace shirakami::occ