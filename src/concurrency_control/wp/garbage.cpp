
#include <xmmintrin.h>

#include "clock.h"

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

namespace shirakami::garbage {

void gc_handle::destroy() {
    while (!val_cont_.empty()) {
        value_type val{};
        val_cont_.try_pop(val);
        if (val.first != nullptr) {
            delete val.first; // NOLINT
            val = {};
        }
    }
    val_cont_.clear();
}

void init() {
    set_flag_manager_end(false);
    set_flag_cleaner_end(false);
    invoke_bg_threads();
}

void fin() {
    set_flag_manager_end(true);
    set_flag_cleaner_end(true);
    join_bg_threads();
}

void work_manager() {
    while (!get_flag_manager_end()) {
        epoch::epoch_t min_step_epoch{epoch::max_epoch};
        epoch::epoch_t min_batch_epoch{epoch::max_epoch};
        for (auto&& se : session_table::get_session_table()) {
            if (se.get_visible()) {
                min_step_epoch = std::min(min_step_epoch, se.get_step_epoch());
                if (se.get_mode() == tx_mode::BATCH) {
                    min_batch_epoch =
                            std::min(min_batch_epoch, se.get_valid_epoch());
                }
            }
        }
        if (min_step_epoch != epoch::max_epoch) {
            set_min_step_epoch(min_step_epoch);
        }
        if (min_batch_epoch != epoch::max_epoch) {
            set_min_batch_epoch(min_batch_epoch);
        }

        sleepMs(PARAM_EPOCH_TIME);
    }
}

void work_cleaner() {
    while (!get_flag_cleaner_end()) {
        // todo
        _mm_pause();
    }
}

} // namespace shirakami::garbage