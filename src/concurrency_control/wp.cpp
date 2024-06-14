
#include <algorithm>
#include <string_view>
#include <vector>

#include "storage.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "database/include/logging.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::wp {

void extract_higher_priori_ltx_info(session* const ti,
                                    wp_meta* const wp_meta_ptr,
                                    wp_meta::wped_type const& wps,
                                    std::string_view const key) {
    // undefined hit process
    auto undefined_hit_process = [ti, wp_meta_ptr,
                                  key](std::size_t against_id) {
        // get mutex for strand
        std::lock_guard<std::shared_mutex> lk{ti->get_mtx_overtaken_ltx_set()};

        /**
          * If this is the first forwarding against this table, 
          * initialize read range.
          */
        auto& target_set =
                std::get<0>(ti->get_overtaken_ltx_set()[wp_meta_ptr]);
        auto& read_range =
                std::get<1>(ti->get_overtaken_ltx_set()[wp_meta_ptr]);
        if (target_set.empty() || !std::get<4>(read_range)) {
            // initialize read range
            std::get<0>(read_range) = key;
            std::get<2>(read_range) = key;
            std::get<4>(read_range) = true;
        } else {
            // already initialize, update read range
            if (key < std::get<0>(read_range)) {
                std::get<0>(read_range) = key;
            } else if (key > std::get<2>(read_range)) {
                std::get<2>(read_range) = key;
            }
        }
        target_set.insert(against_id);
    };

    // check living wp
    for (auto&& wped : wps) {
        if (wped.second != 0) {
            if (wped.second < ti->get_long_tx_id()) {
                // this tx decides that wped.second tx may be the forwarding target.

                // hit
                undefined_hit_process(wped.second);
            }
        }
    }

    // check committed wp at my start epoch
    // todo early abort
    {
        std::shared_lock<std::shared_mutex> lk{
                wp_meta_ptr->get_mtx_wp_result_set()};
        for (auto&& wp_result_itr = wp_meta_ptr->get_wp_result_set().begin();
             wp_result_itr != wp_meta_ptr->get_wp_result_set().end();
             ++wp_result_itr) {
            auto wp_result_id =
                    wp::wp_meta::wp_result_elem_extract_id((*wp_result_itr));
            auto wp_result_was_committed =
                    wp::wp_meta::wp_result_elem_extract_was_committed(
                            (*wp_result_itr));
            auto wp_result_epoch =
                    wp::wp_meta::wp_result_elem_extract_epoch((*wp_result_itr));
            if (ti->get_long_tx_id() > wp_result_id &&
                wp_result_was_committed &&
                ti->get_valid_epoch() == wp_result_epoch) {

                // hit
                undefined_hit_process(wp_result_id);
            }
        }
    }
}

void extract_higher_priori_ltx_info(session* const ti,
                                    wp_meta* const wp_meta_ptr,
                                    wp_meta::wped_type const& wps) {
    // undefined hit process
    auto undefined_hit_process = [ti, wp_meta_ptr](std::size_t against_id) {
        // get mutex
        std::lock_guard<std::shared_mutex> lk{ti->get_mtx_overtaken_ltx_set()};

        /**
         * If this is the first forwarding against this table, 
         * initialize read range.
         */
        auto& target_set =
                std::get<0>(ti->get_overtaken_ltx_set()[wp_meta_ptr]);
        target_set.insert(against_id);
    };

    for (auto&& wped : wps) {
        if (wped.second != 0) {
            if (wped.second < ti->get_long_tx_id()) {
                // this tx decides that wped.second tx may be the forwarding target.
                undefined_hit_process(wped.second);
            }
        }
    }

    // check committed wp at my start epoch
    // todo early abort
    {
        std::shared_lock<std::shared_mutex> lk{
                wp_meta_ptr->get_mtx_wp_result_set()};
        for (auto&& wp_result_itr = wp_meta_ptr->get_wp_result_set().begin();
             wp_result_itr != wp_meta_ptr->get_wp_result_set().end();
             ++wp_result_itr) {
            auto wp_result_id =
                    wp::wp_meta::wp_result_elem_extract_id((*wp_result_itr));
            auto wp_result_was_committed =
                    wp::wp_meta::wp_result_elem_extract_was_committed(
                            (*wp_result_itr));
            auto wp_result_epoch =
                    wp::wp_meta::wp_result_elem_extract_epoch((*wp_result_itr));
            if (ti->get_long_tx_id() > wp_result_id &&
                wp_result_was_committed &&
                ti->get_valid_epoch() == wp_result_epoch) {

                // hit
                undefined_hit_process(wp_result_id);
            }
        }
    }
}

Status fin() {
    if (!get_initialized()) { return Status::WARN_NOT_INIT; }

    set_finalizing(true);
    Storage storage = get_page_set_meta_storage();
    auto rc = delete_storage(storage);
    if (Status::OK != rc) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix << rc << "unreachable path.";
        return Status::ERR_FATAL;
    }
    set_page_set_meta_storage(initial_page_set_meta_storage);
    set_initialized(false);
    set_finalizing(false);
    return Status::OK;
}

Status find_page_set_meta(Storage st, page_set_meta*& ret) {
    Storage page_set_meta_storage = get_page_set_meta_storage();
    std::string_view page_set_meta_storage_view = {
            reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
            sizeof(page_set_meta_storage)};
    std::string_view storage_view = {
            reinterpret_cast<const char*>(&st), // NOLINT
            sizeof(st)};
    std::pair<page_set_meta**, std::size_t> out{};
    auto rc{yakushima::get<page_set_meta*>(page_set_meta_storage_view, // NOLINT
                                           storage_view, out)};
    if (rc != yakushima::status::OK) {
        ret = nullptr;
        return Status::WARN_NOT_FOUND;
    }
    ret = reinterpret_cast<page_set_meta*>(out.first); // NOLINT
    // by inline optimization
    return Status::OK;
}

Status find_read_by(Storage const st, range_read_by_long*& ret) {
    page_set_meta* psm{};
    auto rc{find_page_set_meta(st, psm)};
    if (rc == Status::WARN_NOT_FOUND) { return rc; }
    ret = psm->get_range_read_by_long_ptr();
    return Status::OK;
}

Status find_wp_meta(Storage st, wp_meta*& ret) {
    page_set_meta* psm{};
    auto rc{find_page_set_meta(st, psm)};
    if (rc == Status::WARN_NOT_FOUND) { return rc; }
    ret = psm->get_wp_meta_ptr();
    return Status::OK;
}

wp_meta::wped_type find_wp(Storage const storage) {
    wp_meta* target_wp_meta{};
    if (find_wp_meta(storage, target_wp_meta) != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "There is no metadata that should be there.: " << storage;
    }

    return target_wp_meta->get_wped();
}

Status init() {
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    if (auto rc{storage::register_storage(storage::wp_meta_storage)};
        rc != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix << rc << ", unreachable path.";
    }
    set_page_set_meta_storage(storage::wp_meta_storage);
    set_initialized(true);
    return Status::OK;
}

Status write_preserve(Token token, std::vector<Storage> storage,
                      std::size_t long_tx_id, epoch::epoch_t valid_epoch) {
    // decide storage form
    // reduce redundant
    auto* ti = static_cast<session*>(token);
    std::sort(storage.begin(), storage.end());
    storage.erase(std::unique(storage.begin(), storage.end()), storage.end());

    ti->get_wp_set().reserve(storage.size());

    for (auto&& wp_target : storage) {
        Storage page_set_meta_storage = get_page_set_meta_storage();
        std::string_view page_set_meta_storage_view = {
                reinterpret_cast<char*>( // NOLINT
                        &page_set_meta_storage),
                sizeof(page_set_meta_storage)};
        std::string_view storage_view = {
                reinterpret_cast<char*>(&wp_target), // NOLINT
                sizeof(wp_target)};
        std::pair<page_set_meta**, std::size_t> out{};
        auto rc{yakushima::get<page_set_meta*>( // NOLINT
                page_set_meta_storage_view,     // NOLINT
                storage_view, out)};

        auto cleanup_process = [ti, long_tx_id]() {
            for (auto&& elem : ti->get_wp_set()) {
                if (Status::OK != elem.second->remove_wp(long_tx_id)) {
                    LOG_FIRST_N(ERROR, 1)
                            << log_location_prefix << "unreachable path";
                    return;
                }
            }
            ti->clean_up();
        };
        if (rc != yakushima::status::OK) {
            cleanup_process();
            // dtor : release wp_mutex
            std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
            ti->get_result_info().set_reason_code(reason_code::UNKNOWN);
            ti->get_result_info().set_storage_name(wp_target);
            return Status::WARN_INVALID_ARGS;
        }
        wp_meta* target_wp_meta =
                (reinterpret_cast<page_set_meta*>(out.first)) // NOLINT
                        ->get_wp_meta_ptr();
        if (target_wp_meta->register_wp(valid_epoch, long_tx_id) !=
            Status::OK) {
            cleanup_process();
            return Status::ERR_FATAL;
        }
        ti->get_wp_set().emplace_back(
                std::make_pair(wp_target, target_wp_meta));
    }

    return Status::OK;
}

} // namespace shirakami::wp