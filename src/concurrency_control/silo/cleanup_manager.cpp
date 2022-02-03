/**
 * @file cleanup_manager.cpp
 * @brief Implementation about cleanup_manager.
 */

#include "clock.h"

#include "include/cleanup_manager.h"
#include "include/session_table.h"
#include "include/snapshot_manager.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

namespace shirakami::cleanup_manager {

void cleanup_manager_func() {

    while (!get_cleanup_manager_thread_end()) {
        /**
         * Dividing by 2 is a heuristic. However, if the timing is not right, 
         * the unhooked record will be delayed by about 2 epochs, so divide it by 2.
         */
        sleepMs(PARAM_EPOCH_TIME / 2); // NOLINT

        shirakami::Token token{};
        while (Status::OK != shirakami::enter(token)) { _mm_pause(); }

        for (auto&& elem : session_table::get_session_table()) {
            auto& handle = elem.get_cleanup_handle();
            auto& cont = handle.get_cont();
            auto& cache = handle.get_cache();

            while (!cont.empty() || cache.second != nullptr) {
                std::string storage{};
                Record* rec_ptr{};
                if (cache.second == nullptr && !cont.try_pop(cache)) {
                    // First cond means cache is null.
                    // Second cond means cont is null.
                    // Note : The cache takes precedence over the container.
                    break;
                }
                storage = cache.first;
                rec_ptr = cache.second;

                if (rec_ptr->get_tidw().get_epoch() <
                    epoch::get_global_epoch()) {
                    // removing process

                    std::string key{};
                    rec_ptr->get_tuple().get_key(key);

                    if (rec_ptr->get_snap_ptr() == nullptr) {
                        // if no snapshot, it can immediately remove.
                        auto* ti = static_cast<session*>(token);
                        yakushima::remove(ti->get_yakushima_token(), storage,
                                          key);
                        ti->get_gc_handle().get_rec_cont().push(rec_ptr);
                    } else {
                        snapshot_manager::remove_rec_cont.push(
                                {storage, rec_ptr});
                    }
                    cache = {"", nullptr}; // clear cache
                } else {
                    break;
                }
            }
        }

        leave(token);
    }
}

} // namespace shirakami::cleanup_manager