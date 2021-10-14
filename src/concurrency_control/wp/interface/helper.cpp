

#include <string_view>

#include "include/helper.h"

#include "concurrency_control/wp/include/batch.h"
#include "concurrency_control/wp/include/epoch_internal.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/wp.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status enter(Token& token) { // NOLINT
    Status ret_status = session_table::decide_token(token);
    if (ret_status != Status::OK) return ret_status;

    yakushima::Token kvs_token{};
    while (yakushima::enter(kvs_token) != yakushima::status::OK) {
        _mm_pause();
    }
    static_cast<session*>(token)->set_yakushima_token(kvs_token);

    return Status::OK;
}

void fin([[maybe_unused]] bool force_shut_down_cpr) try {
    if (!get_initialized()) { return; }

    epoch::fin();
    delete_all_records();
    wp::fin(); // note: this use yakushima.
    yakushima::fin();
    set_initialized(false);
} catch (std::exception& e) {
    std::cerr << "fin() : " << e.what() << std::endl;
}

Status
init([[maybe_unused]] bool enable_recovery,
     [[maybe_unused]] const std::string_view log_directory_path) { // NOLINT
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    // about cc
    session_table::init_session_table(enable_recovery);
    //epoch::invoke_epoch_thread();

    // about index
    // pre condition : before wp::init() because wp::init() use yakushima function.
    yakushima::init();

    // about wp
    auto ret = wp::init();
    if (ret != Status::OK) { return ret; }

    // about epoch
    epoch::init();

    set_initialized(true);
    return Status::OK;
}

Status leave(Token const token) { // NOLINT
    for (auto&& itr : session_table::get_session_table()) {
        if (&itr == static_cast<session*>(token)) {
            if (itr.get_visible()) {
                yakushima::leave(
                        static_cast<session*>(token)->get_yakushima_token());
                itr.set_tx_began(false);
                itr.set_visible(false);
                return Status::OK;
            }
            return Status::WARN_NOT_IN_A_SESSION;
        }
    }
    return Status::WARN_INVALID_ARGS;
}

Status tx_begin(Token const token, bool const read_only, bool const for_batch,
                std::vector<Storage> write_preserve) { // NOLINT
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        if (for_batch) {
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
            auto batch_id = wp::batch::get_counter();

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
                if (elem_ptr == nullptr) {
                    for (auto&& elem : wped) {
                        if (Status::OK != elem->remove_wp(batch_id)) {
                            LOG(FATAL) << "vanish registered wp.";
                            std::abort();
                        }
                    }
                    ti->get_wp_set().clear();
                    // dtor : release wp_mutex
                    return Status::ERR_FAIL_WP;
                }
                wp::wp_meta* target_wp_meta = *elem_ptr;
                target_wp_meta->register_wp(valid_epoch, batch_id);
                wped.emplace_back(
                        target_wp_meta); // for fast cleanup at failure
                ti->get_wp_set().emplace_back(wp_target);
            }

            // inc batch counter
            wp::batch::set_counter(batch_id + 1);

            // dtor : release wp_mutex
        }
        ti->set_tx_began(true);
        ti->set_read_only(read_only);
        ti->get_write_set().set_for_batch(for_batch);
    } else {
        return Status::WARN_ALREADY_BEGIN;
    }

    return Status::OK;
}

} // namespace shirakami