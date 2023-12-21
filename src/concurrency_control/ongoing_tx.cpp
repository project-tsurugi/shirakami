
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

namespace shirakami {

#ifndef WAITING_BYPASS
#define WAITING_BYPASS 1
#endif

bool ongoing_tx::exist_id(std::size_t id) {
    std::shared_lock<std::shared_mutex> lk{mtx_};
    for (auto&& elem : tx_info_) {
        if (std::get<ongoing_tx::index_id>(elem) == id) { return true; }
    }
    return false;
}

Status ongoing_tx::waiting_bypass(session* ti) {
    // @pre shared lock to tx_info_

    auto exist_living_wait_for = [](session* target_ti) {
        auto wait_for{target_ti->extract_wait_for()};
        for (auto&& elem : tx_info_) {
            auto the_tx_id = std::get<ongoing_tx::index_id>(elem);
            auto f_itr = wait_for.find(the_tx_id);
            if (f_itr != wait_for.end()) { return true; }
        }
        return false;
    };

    /**
     * 現時点で前置候補の LTX群。これらで走行中のものをルート以外バイパスする。
    */
    auto wait_for{ti->extract_wait_for()};
    std::set<std::tuple<std::size_t, session*>> bypass_target{};
    for (auto&& elem : tx_info_) {
        auto the_tx_id = std::get<ongoing_tx::index_id>(elem);
        auto f_itr = wait_for.find(the_tx_id);
        if (f_itr != wait_for.end()) {
            // found
            auto* token = std::get<ongoing_tx::index_session>(elem);

#ifndef WAITING_BYPASS_TO_ROOT
            // check exist living wait for, for not to remove path to root.
            if (!exist_living_wait_for(token)) {
                // not bypass for tree root
                continue;
            }
#endif

            bypass_target.insert(std::make_tuple(the_tx_id, token));
            // set valid epoch if need
            if (ti->get_valid_epoch() > token->get_valid_epoch()) {
                // update valid epoch and check rub violation
                if (ti->get_read_version_max_epoch() >=
                    token->get_valid_epoch()) {
                    // rub violation
                    ti->set_result(
                            reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION);
                    return Status::ERR_CC;
                } // else success.
                ti->set_valid_epoch(token->get_valid_epoch());
            }
        }
    }

    // register whether forwarding
    if (!bypass_target.empty()) { ti->set_is_forwarding(true); }

    // remove bypassed target information
    {
        // get mutex for overtaken ltx set
        std::lock_guard<std::shared_mutex> lk{ti->get_mtx_overtaken_ltx_set()};

        for (auto ols_itr = ti->get_overtaken_ltx_set().begin();
             // for each storage
             ols_itr != ti->get_overtaken_ltx_set().end();) {
            auto& overtaken_ltx_ids = std::get<0>(ols_itr->second);
            // find bypass
            std::set<std::size_t> erase_targets;
            for (auto&& bt_itr : bypass_target) {
                auto find_itr = overtaken_ltx_ids.find(std::get<0>(bt_itr));
                if (find_itr != overtaken_ltx_ids.end()) {
                    // found, bypass
                    erase_targets.insert(std::get<0>(bt_itr));
                }
            }
            // erase by erase_targets
            for (auto&& erase_elem : erase_targets) {
                overtaken_ltx_ids.erase(erase_elem);
            }

            // if it is empty, clear the element
            if (overtaken_ltx_ids.empty()) {
                ols_itr = ti->get_overtaken_ltx_set().erase(ols_itr);
            } else {
                // increment itr
                ++ols_itr;
            }
        }
    }

    // register bypass target information
    for (auto&& elem : tx_info_) {
        auto the_tx_id = std::get<ongoing_tx::index_id>(elem);
        // find from bypass targets
        for (auto&& bt_itr : bypass_target) {
            auto bypass_id = std::get<0>(bt_itr);
            if (bypass_id == the_tx_id) {
                // hit, register
                {
                    auto* bypass_token = std::get<1>(bt_itr);
                    // get mutex for overtaken ltx set
                    std::lock_guard<std::shared_mutex> lk{
                            ti->get_mtx_overtaken_ltx_set()};
                    std::shared_lock<std::shared_mutex> lk2{
                            bypass_token->get_mtx_overtaken_ltx_set()};
                    for (auto&& ols_elem :
                         bypass_token->get_overtaken_ltx_set()) {
                        auto* wp_meta_ptr = ols_elem.first;
                        // find local set
                        auto local_ols_itr =
                                ti->get_overtaken_ltx_set().find(wp_meta_ptr);
                        // overwrite or insert
                        if (local_ols_itr !=
                            ti->get_overtaken_ltx_set().end()) {
                            // hit and overwrite, merge, copy
                            // merge ids
                            auto& ols_ids = std::get<0>(local_ols_itr->second);
                            auto& merge_source_ids =
                                    std::get<0>(ols_elem.second);
                            for (auto id : merge_source_ids) {
                                ols_ids.insert(id);
                            }
                            // merge read range, about left endpoint
                            std::string left_end_source =
                                    std::get<0>(std::get<1>(ols_elem.second));
                            std::string& left_end_base = std::get<0>(
                                    std::get<1>(local_ols_itr->second));
                            if (left_end_source < left_end_base) {
                                left_end_base = left_end_source;
                            }
                            // about right endpoint
                            std::string right_end_source =
                                    std::get<2>(std::get<1>(ols_elem.second));
                            std::string& right_end_base = std::get<2>(
                                    std::get<1>(local_ols_itr->second));
                            if (right_end_base < right_end_source) {
                                right_end_base = right_end_source;
                            }
                        } else {
                            // not hit and create(copy) element
                            ti->get_overtaken_ltx_set()[wp_meta_ptr] =
                                    ols_elem.second;
                        }
                    }
                }
                // success and break;
                break;
            }
        }
    }

    return Status::OK;
}

bool ongoing_tx::exist_wait_for(session* ti, Status& out_status) {
    out_status = Status::OK; // initialize arg
    std::size_t id = ti->get_long_tx_id();
    bool has_wp = !ti->get_wp_set().empty();
    auto wait_for = ti->extract_wait_for();
    // check local write set
    std::set<Storage> st_set{};
    // create and compaction about storage set
    ti->get_write_set().get_storage_set(st_set);
    if (!ti->get_requested_commit()) {
        // first request, so update wp
        long_tx::update_wp_at_commit(ti, st_set);
    }

    if (!wait_for.empty()) {
        ti->set_was_considering_forwarding_at_once(true);

        // check boundary wait
        {
            std::shared_lock<std::shared_mutex> lk{mtx_};
            for (auto&& elem : tx_info_) {
                // check overwrites
                if (std::get<ongoing_tx::index_id>(elem) < id) {
                    if (wait_for.find(std::get<ongoing_tx::index_id>(elem)) !=
                        wait_for.end()) {
                        // wait_for hit.
                        /**
                         * boundary wait 確定.
                         * waiting by pass: 自分が（前置するかもしれなくて）待つ相手x1
                         * に対する前置を確定するとともに、x1 が前置する相手に前置する。
                         * これは待ち確認のたびにパスを一つ短絡化するため、
                         * get_requested_commit() の確認を噛ませていない。
                         */
                        bool do_waiting_bypass;  // NOLINT
#if WAITING_BYPASS
# ifdef WAITING_BYPASS_TO_ROOT
                        do_waiting_bypass = true;
# else
                        /**
                         * https://github.com/project-tsurugi/tsurugi-issues/issues/438#issuecomment-1839876140
                         * ルートになるまでパスを縮めてはいけない。
                         */
                        do_waiting_bypass = (wait_for.size() > 2);
# endif
#else
                        do_waiting_bypass = false;
#endif
                        if (do_waiting_bypass) {
                            out_status = waiting_bypass(ti);
                        }
                        return true;
                    }
                } else {
                    // considering for only high priori ltx
                    break;
                }
            }
        }
    }

    if (ti->get_was_considering_forwarding_at_once()) {
        // at least, this tx was considering forwarding so needs to check read
        // wait.
        // check about write
        if (has_wp) {
            // check potential read-anti and read area for each write storage
            bool ret = read_plan::check_potential_read_anti(id, st_set);
            if (!ret) {
                // no need to read wait and it can try IWR
                return false;
            }
            // should wait read except write only

            // check write only
            bool write_only = ti->is_write_only_ltx_now();
            return !write_only;
            // write only true: no need to wait
            // write only false: not write only and may have high priori read
        }
    }

    return false;
}

void ongoing_tx::push(tx_info_elem_type const ti) {
    std::lock_guard<std::shared_mutex> lk{mtx_};
    if (tx_info_.empty()) {
        set_lowest_epoch(std::get<ongoing_tx::index_epoch>(ti));
    }
    tx_info_.emplace_back(ti);
}

void ongoing_tx::push_bringing_lock(tx_info_elem_type const ti) {
    if (tx_info_.empty()) {
        set_lowest_epoch(std::get<ongoing_tx::index_epoch>(ti));
    }
    tx_info_.emplace_back(ti);
}

void ongoing_tx::remove_id(std::size_t const id) {
    std::lock_guard<std::shared_mutex> lk{mtx_};
    epoch::epoch_t lep{0};
    bool first{true};
    bool erased{false};
    for (auto it = tx_info_.begin(); it != tx_info_.end();) { // NOLINT
        if (!erased && std::get<ongoing_tx::index_id>(*it) == id) {
            tx_info_.erase(it);
            // TODO: it = ?
            erased = true;
        } else {
            // update lowest epoch
            if (first) {
                lep = std::get<ongoing_tx::index_epoch>(*it);
                first = false;
            } else {
                if (std::get<ongoing_tx::index_epoch>(*it) < lep) {
                    lep = std::get<ongoing_tx::index_epoch>(*it);
                }
            }

            ++it;
        }
    }
    if (tx_info_.empty()) {
        set_lowest_epoch(0);
    } else {
        set_lowest_epoch(lep);
    }
    if (!erased) { LOG(ERROR) << log_location_prefix << "unreachable path."; }
}

} // namespace shirakami