
#include <algorithm>
#include <string_view>
#include <vector>

#include "storage.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::wp {

void extract_higher_priori_ltx_info(session* const ti,
                                    wp_meta* const wp_meta_ptr,
                                    wp_meta::wped_type const& wps) {
    for (auto&& wped : wps) {
        if (wped.second != 0) {
            if (wped.second < ti->get_long_tx_id()) {
                ti->get_overtaken_ltx_set()[wp_meta_ptr].insert(wped.second);
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
        LOG(FATAL) << rc;
        std::abort();
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
    auto rc{yakushima::get<page_set_meta*>(page_set_meta_storage_view,
                                           storage_view, out)};
    if (rc != yakushima::status::OK) {
        ret = nullptr;
        return Status::WARN_NOT_FOUND;
    }
    ret = *out.first;
    return Status::OK;
}

Status find_read_by(Storage const st, range_read_by_long*& ret) {
    page_set_meta* psm{};
    auto rc{find_page_set_meta(st, psm)};
    if (rc == Status::WARN_NOT_FOUND) { return rc; }
    ret = psm->get_range_read_by_long_ptr();
    return Status::OK;
}

Status find_read_by(Storage const st, point_read_by_long*& ret) {
    page_set_meta* psm{};
    auto rc{find_page_set_meta(st, psm)};
    if (rc == Status::WARN_NOT_FOUND) { return rc; }
    ret = psm->get_point_read_by_long_ptr();
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
        LOG(FATAL) << "There is no metadata that should be there.: " << storage;
    }

    return target_wp_meta->get_wped();
}

Status init() {
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    if (auto rc{storage::register_storage(storage::wp_meta_storage)};
        rc != Status::OK) {
        LOG(FATAL) << rc;
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
        auto rc{yakushima::get<page_set_meta*>(page_set_meta_storage_view,
                                               storage_view, out)};

        auto cleanup_process = [ti, long_tx_id]() {
            for (auto&& elem : ti->get_wp_set()) {
                if (Status::OK != elem.second->remove_wp(long_tx_id)) {
                    LOG(ERROR) << "programming error";
                    return;
                }
            }
            ti->clean_up();
        };
        if (rc != yakushima::status::OK) {
            cleanup_process();
            // dtor : release wp_mutex
            return Status::ERR_FAIL_WP;
        }
        wp_meta* target_wp_meta = (*out.first)->get_wp_meta_ptr();
        if (Status::OK !=
            target_wp_meta->register_wp(valid_epoch, long_tx_id)) {
            cleanup_process();
            return Status::ERR_FAIL_WP;
        }
        ti->get_wp_set().emplace_back(
                std::make_pair(wp_target, target_wp_meta));
    }

    return Status::OK;
}

} // namespace shirakami::wp