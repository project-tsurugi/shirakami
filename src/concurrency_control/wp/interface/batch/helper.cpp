
#include "concurrency_control/wp/include/batch.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"

#include "concurrency_control/wp/include/tuple_local.h"

namespace shirakami::batch {

Status tx_begin(session* const ti,
                std::vector<Storage> write_preserve) { // NOLINT
    /**
      * preserve wp set.
      * This is preparing for the calculation before the 
      * critical section for speedup.
      */
    ti->get_wp_set().reserve(write_preserve.size());
    std::vector<wp::wp_meta*> wped{};
    wped.reserve(write_preserve.size());

    // get wp mutex
    auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());

    // get batch id
    auto batch_id = shirakami::wp::batch::get_counter();

    // compute future epoch
    auto valid_epoch = epoch::get_global_epoch() + 1;

    // do write preserve
    for (auto&& wp_target : write_preserve) {
        Storage page_set_meta_storage = wp::get_page_set_meta_storage();
        std::string_view page_set_meta_storage_view = {
                reinterpret_cast<char*>( // NOLINT
                        &page_set_meta_storage),
                sizeof(page_set_meta_storage)};
        std::string_view storage_view = {
                reinterpret_cast<char*>(&wp_target), // NOLINT
                sizeof(wp_target)};
        auto* elem_ptr = std::get<0>(yakushima::get<wp::wp_meta*>(
                page_set_meta_storage_view, storage_view));
        auto cleanup_process = [ti, &wped, batch_id]() {
            for (auto&& elem : wped) {
                if (Status::OK != elem->remove_wp(batch_id)) {
                    LOG(FATAL) << "vanish registered wp.";
                    std::abort();
                }
            }
            ti->clean_up();
        };
        if (elem_ptr == nullptr) {
            cleanup_process();
            // dtor : release wp_mutex
            return Status::ERR_FAIL_WP;
        }
        wp::wp_meta* target_wp_meta = *elem_ptr;
        if (Status::OK != target_wp_meta->register_wp(valid_epoch, batch_id)) {
            cleanup_process();
            return Status::ERR_FAIL_WP;
        }
        wped.emplace_back(target_wp_meta); // for fast cleanup at failure
        ti->get_wp_set().emplace_back(wp_target);
    }

    // inc batch counter
    wp::batch::set_counter(batch_id + 1);

    ti->set_batch_id(batch_id);
    ti->set_mode(tx_mode::BATCH);
    ti->set_valid_epoch(valid_epoch);

    return Status::OK;
    // dtor : release wp_mutex
}

} // namespace shirakami::batch