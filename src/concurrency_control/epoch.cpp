
#include "clock.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/epoch_internal.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "database/include/logging.h"

#ifdef PWAL

#include "datastore/limestone/include/datastore.h"
#include "datastore/limestone/include/limestone_api_helper.h"

#endif

#include "shirakami/logging.h"

#include "glog/logging.h"

namespace shirakami::epoch {

inline void check_epoch_load_and_update_idle_living_tx() {
    auto ce{epoch::get_global_epoch()};
    for (auto&& itr : session_table::get_session_table()) {
        if (itr.get_operating().load(std::memory_order_acquire) == 0) {
            // this session is not processing now.
            if (itr.get_step_epoch() < ce) { itr.set_step_epoch(ce); }
        }
    }
}

inline void compute_and_set_cc_safe_ss_epoch() {
    // compute cc safe ss epoch
    // get read lock and block ending of highest priori ltx
    epoch_t result_epoch{0};
    {
        std::shared_lock<std::shared_mutex> lk_ongo{ongoing_tx::get_mtx()};

        // ongoing_tx::tx_info may contain RTX, so remove RTX here
        ongoing_tx::tx_info_type ongoing_ltx{};
        ongoing_ltx.reserve(ongoing_tx::get_tx_info().size());
        std::copy_if(ongoing_tx::get_tx_info().begin(), ongoing_tx::get_tx_info().end(),
                     std::back_inserter(ongoing_ltx),
                     [](const auto& elem){
                             return std::get<ongoing_tx::index_session>(elem)->get_tx_type() !=
                                     transaction_options::transaction_type::READ_ONLY;});

        if (ongoing_ltx.empty()) {
            // set cc safe ss epoch
            set_cc_safe_ss_epoch(get_global_epoch() + 1);
            return;
        }
        // exist ltx
        for (auto& elem : ongoing_ltx) {
            auto* ti = std::get<ongoing_tx::index_session>(elem);
            // initialize result_epoch
            if (result_epoch == 0) { result_epoch = ti->get_valid_epoch(); }
            // acquire read lock about overtaken ltx set
            {
                std::shared_lock<std::shared_mutex> lk_ols{
                        ti->get_mtx_overtaken_ltx_set()};
                // check
                if (ti->get_overtaken_ltx_set().empty()) {
                    // no forwarding
                    if (ti->get_valid_epoch() < result_epoch) {
                        result_epoch = ti->get_valid_epoch();
                    }
                    // else, already initialize for non zero
                    continue;
                }
                // exist forwording, compute return epoch
                for (auto&& oe : ti->get_overtaken_ltx_set()) {
                    wp::wp_meta* wp_meta_ptr{oe.first};
                    // get read lock
                    std::shared_lock<std::shared_mutex> lk{
                            wp_meta_ptr->get_mtx_wp_result_set()};
                    for (auto&& wp_result_itr =
                                 wp_meta_ptr->get_wp_result_set().begin();
                         wp_result_itr !=
                         wp_meta_ptr->get_wp_result_set().end();
                         ++wp_result_itr) {
                        // prepare committed information
                        auto wp_result_id =
                                wp::wp_meta::wp_result_elem_extract_id(
                                        (*wp_result_itr));
                        auto wp_result_epoch =
                                wp::wp_meta::wp_result_elem_extract_epoch(
                                        (*wp_result_itr));
                        auto wp_result_was_committed = wp::wp_meta::
                                wp_result_elem_extract_was_committed(
                                        (*wp_result_itr));
                        if (wp_result_was_committed) {
                            /**
                             * the target ltx was commited, so it needs to check.
                             */
                            for (auto&& hid : std::get<0>(oe.second)) {
                                if (wp_result_id == hid) {
                                    if (wp_result_epoch < result_epoch) {
                                        result_epoch = wp_result_epoch;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // set cc safe ss epoch
    set_cc_safe_ss_epoch(result_epoch);
}

void epoch_thread_work() {
    while (!get_epoch_thread_end()) {
        sleepUs(epoch::get_global_epoch_time_us());
        {
            // coordination with ltx
            auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());
            std::unique_lock<std::mutex> lk{get_ep_mtx()};
            auto ptp{epoch::get_perm_to_proc()};
            // -1: ptp invalid
            // 0: no work to proceed
            if (ptp == 0) { continue; } // no work
            if (ptp < -1) {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << log_location_prefix
                        << "unreachable path.";
                return;
            }
            // change epoch
            auto new_epoch{get_global_epoch() + 1};
            set_global_epoch(new_epoch);
            compute_and_set_cc_safe_ss_epoch();
#ifdef PWAL
            // change also datastore's epoch
            switch_epoch(shirakami::datastore::get_datastore(), new_epoch);
#endif
            // compute for debug tools
            if (ptp > 0) {
                // ptp allow epoch inclement
                epoch::set_perm_to_proc(ptp - 1);
            }
            // dtor : release wp_mutex
        }
        check_epoch_load_and_update_idle_living_tx();
    }
}

void fin() {
    set_epoch_thread_end(true);
    join_epoch_thread();
}

void init([[maybe_unused]] std::size_t const epoch_time) {
// set global epoch time
#if PARAM_EPOCH_TIME > 0
    set_global_epoch_time_us(PARAM_EPOCH_TIME);
#else
    set_global_epoch_time_us(epoch_time);
#endif

    // initialize epoch tool
    set_perm_to_proc(ptp_init_val);

    // invoke epoch thread
    invoke_epoch_thread();
}

void invoke_epoch_thread() {
    // initialize
    set_epoch_thread_end(false);

    // invoking epoch thread
    epoch_thread = std::thread(epoch_thread_work);
}

} // namespace shirakami::epoch
