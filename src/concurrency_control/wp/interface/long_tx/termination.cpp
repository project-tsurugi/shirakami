
#include <algorithm>
#include <string_view>
#include <vector>

#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

#include "concurrency_control/include/tuple_local.h"

#include "glog/logging.h"

namespace shirakami::long_tx {

void register_wp_result_and_remove_wps(session* ti) {
    for (auto&& elem : ti->get_wp_set()) {
        Storage storage = elem.first;
        Storage page_set_meta_storage = wp::get_page_set_meta_storage();
        std::string_view page_set_meta_storage_view = {
                reinterpret_cast<char*>( // NOLINT
                        &page_set_meta_storage),
                sizeof(page_set_meta_storage)};
        std::string_view storage_view = {
                reinterpret_cast<char*>(&storage), // NOLINT
                sizeof(storage)};
        std::pair<wp::page_set_meta**, std::size_t> out{};
        auto rc{yakushima::get<wp::page_set_meta*>(page_set_meta_storage_view,
                                                   storage_view, out)};
        if (rc != yakushima::status::OK) {
            LOG(ERROR) << "programing error: " << rc;
            return;
        }
        if (Status::OK !=
            (*out.first)
                    ->get_wp_meta_ptr()
                    ->register_wp_result_and_remove_wp(ti->get_valid_epoch(),
                                                       ti->get_batch_id())) {
            LOG(FATAL);
        }
    }
}

void cleanup_process(session* const ti) {
    // global effect
    register_wp_result_and_remove_wps(ti);
    ongoing_tx::remove_id(ti->get_batch_id());

    // local effect
    ti->clean_up();
    /**
     * When you execute leave (session), perform self-abort processing for 
     * cleanup. In OCC mode, the effect is idempotent. If it is in BATCH 
     * mode, it is not idempotent, so a bug will occur.
     * 
     */
    ti->set_tx_type(TX_TYPE::SHORT);
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

Status abort(session* const ti) { // NOLINT
    cancel_flag_inserted_records(ti);

    // about tx state
    // this should before clean up
    ti->set_tx_state_if_valid(TxState::StateKind::ABORTED);

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
    tid_word ctid{};
    compute_tid(ti, ctid);

    auto process = [ti](std::pair<Record* const, write_set_obj>& wse,
                        tid_word ctid) {
        auto* rec_ptr = std::get<0>(wse);
        auto&& wso = std::get<1>(wse);
        switch (wso.get_op()) {
            case OP_TYPE::INSERT: {
                // unlock and set ctid
                rec_ptr->set_tid(ctid);
                break;
            }
            case OP_TYPE::DELETE: {
                ctid.set_latest(false);
                ctid.set_absent(true);
                [[fallthrough]];
            }
            case OP_TYPE::UPDATE:
            case OP_TYPE::UPSERT: {
                // lock record
                rec_ptr->get_tidw_ref().lock();
                tid_word pre_tid{rec_ptr->get_tidw_ref().get_obj()};

                if (ti->get_valid_epoch() > pre_tid.get_epoch()) {
                    // case: first of list
                    std::string vb{};
                    if (wso.get_op() != OP_TYPE::DELETE) { wso.get_value(vb); }
                    version* new_v{new version( // NOLINT
                            vb, rec_ptr->get_latest())};
                    // prepare tid for old version
                    pre_tid.set_absent(false);
                    pre_tid.set_latest(false);
                    pre_tid.set_lock(false);
                    // set old version tid
                    rec_ptr->get_latest()->set_tid(pre_tid);
                    // set latest
                    rec_ptr->set_latest(new_v);
                    // unlock and set ctid
                    rec_ptr->set_tid(ctid);
                } else if (ti->get_valid_epoch() == pre_tid.get_epoch()) {
                    rec_ptr->get_tidw_ref().unlock();
                } else {
                    // case: middle of list
                    version* pre_ver{rec_ptr->get_latest()};
                    version* ver{rec_ptr->get_latest()->get_next()};
                    tid_word tid{ver->get_tid()};
                    for (;;) {
                        if (tid.get_epoch() < ti->get_valid_epoch()) {
                            std::string vb{};
                            if (wso.get_op() != OP_TYPE::DELETE) {
                                wso.get_value(vb);
                            }
                            version* new_v{new version(ctid, vb, ver)};
                            pre_ver->set_next(new_v);
                            break;
                        }
                        if (tid.get_epoch() == ti->get_valid_epoch()) {
                            // para (partial order) write, invisible write
                            break;
                        }
                        pre_ver = ver;
                        ver = ver->get_next();
                        tid = ver->get_tid();
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
        process(wso, ctid);
    }
}

void register_read_by(session* const ti) {
    // point read
    for (auto&& elem : ti->get_point_read_by_bt_set()) {
        elem->push({ti->get_valid_epoch(), ti->get_batch_id()});
    }

    // range read
    for (auto&& elem : ti->get_range_read_by_bt_set()) {
        std::get<0>(elem)->push({ti->get_valid_epoch(), ti->get_batch_id(),
                                 std::get<1>(elem), std::get<2>(elem),
                                 std::get<3>(elem), std::get<4>(elem)});
    }
}

void prepare_commit(session* const ti) {
    // optimizations
    // shrink read_by_set
    auto& rbset = ti->get_point_read_by_bt_set();
    std::sort(rbset.begin(), rbset.end());
    rbset.erase(std::unique(rbset.begin(), rbset.end()), rbset.end());
}

Status verify_read_by(session* const ti) {
    auto this_epoch = ti->get_valid_epoch();
    auto this_id = ti->get_batch_id();
    for (auto&& wso : ti->get_write_set().get_ref_cont_for_bt()) {
        // for ltx
        point_read_by_bt* rbp{};
        auto rc{wp::find_read_by(wso.second.get_storage(), rbp)};
        if (rc == Status::OK) {
            if (rbp->is_exist(this_epoch, this_id)) {
                return Status::ERR_VALIDATION;
            }
        } else {
            LOG(ERROR) << "programming error";
            return Status::ERR_FATAL;
        }

        // for stx
        auto* rec_ptr{wso.first};
        if (ti->get_valid_epoch() <= rec_ptr->get_read_by().get_max_epoch()) {
            // this will break commited stx's read
            return Status::ERR_VALIDATION;
        }

        if (wso.second.get_op() == OP_TYPE::INSERT ||
            wso.second.get_op() == OP_TYPE::DELETE) {
            range_read_by_bt* rrbp{};
            auto rc{wp::find_read_by(wso.second.get_storage(), rrbp)};
            if (rc == Status::OK) {
                std::string keyb{};
                wso.first->get_key(keyb);
                auto rb{rrbp->get(this_epoch, keyb)};

                if (rb != range_read_by_bt::body_elem_type{}) {
                    return Status::ERR_VALIDATION;
                }
            } else {
                LOG(ERROR) << "programming error";
                return Status::ERR_FATAL;
            }
        }
    }

    // for overtaken set

    auto gc_threshold = ongoing_tx::get_lowest_epoch();
    for (auto&& oe : ti->get_overtaken_ltx_set()) {
        wp::wp_meta* wp_meta_ptr{oe.first};
        std::lock_guard<std::shared_mutex> lk{
                wp_meta_ptr->get_mtx_wp_result_set()};
        bool is_first_item_before_gc_threshold{true};
        for (auto&& wp_result_itr = wp_meta_ptr->get_wp_result_set().begin();
             wp_result_itr != wp_meta_ptr->get_wp_result_set().end();) {
            for (auto&& hid : oe.second) {
                if ((*wp_result_itr).second == hid) {
                    // the itr show overtaken ltx
                    if ((*wp_result_itr).first < ti->get_valid_epoch()) {
                        // this tx should have read the result of the ltx.
                        return Status::ERR_VALIDATION;
                    }
                    // verify success
                    break;
                }
            }

            // not match. check gc
            if ((*wp_result_itr).first < gc_threshold) {
                // should gc
                if (is_first_item_before_gc_threshold) {
                    // not remove
                    is_first_item_before_gc_threshold = false;
                    ++wp_result_itr;
                } else {
                    // remove
                    wp_result_itr = wp_meta_ptr->get_wp_result_set().erase(
                            wp_result_itr);
                }
            } else {
                // else. should not gc
                ++wp_result_itr;
            }
        }
    }

    return Status::OK;
}

Status check_wait_for_preceding_bt(session* const ti) {
    if (ongoing_tx::exist_preceding_id(ti->get_batch_id())) {
        return Status::WARN_WAITING_FOR_OTHER_TX;
    }
    return Status::OK;
}

extern Status commit(session* const ti, // NOLINT
                     [[maybe_unused]] commit_param* const cp) {
    // check premature
    if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }

    /**
     * WP2: If it is possible to prepend the order, it waits for a transaction 
     * with a higher priority than itself to finish the operation.
     */
    prepare_commit(ti);
    auto rc = check_wait_for_preceding_bt(ti);
    if (rc != Status::OK) { return Status::WARN_WAITING_FOR_OTHER_TX; }

    // verify read by
    rc = verify_read_by(ti);
    if (rc == Status::ERR_VALIDATION) {
        abort(ti);
        return Status::ERR_VALIDATION;
    }

    register_read_by(ti);
    expose_local_write(ti);

    // todo enhancement
    /**
     * Sort by wp and then globalize the local write set. 
     * Eliminate wp from those that have been globalized in wp units.
     */


    // about transaction state
    // this should before clean up
    ti->set_tx_state_if_valid(TxState::StateKind::DURABLE);

    // clean up
    cleanup_process(ti);

    return Status::OK;
}

} // namespace shirakami::long_tx