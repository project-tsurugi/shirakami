
#include <algorithm>
#include <string_view>
#include <vector>

#include "concurrency_control/wp/include/tuple_local.h"

#include "include/wp.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami::wp {

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

Status find_wp_meta(Storage st, wp_meta*& ret) {
    Storage page_set_meta_storage = get_page_set_meta_storage();
    std::string_view page_set_meta_storage_view = {
            reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
            sizeof(page_set_meta_storage)};
    std::string_view storage_view = {
            reinterpret_cast<const char*>(&st), // NOLINT
            sizeof(st)};
    auto* elem_ptr = std::get<0>(
            yakushima::get<wp_meta*>(page_set_meta_storage_view, storage_view));

    if (elem_ptr == nullptr) {
        ret = nullptr;
        return Status::WARN_NOT_FOUND;
    }
    ret = *elem_ptr;
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

    Storage ret_storage{};
    auto rc = register_storage(ret_storage);
    if (Status::OK != rc) {
        LOG(FATAL) << rc;
        std::abort();
    }
    set_page_set_meta_storage(ret_storage);
    set_initialized(true);
    return Status::OK;
}

Status write_preserve(session* const ti, std::vector<Storage> storage,
                      std::size_t batch_id, epoch::epoch_t valid_epoch) {
    // decide storage form
    std::sort(storage.begin(), storage.end());
    storage.erase(std::unique(storage.begin(), storage.end()), storage.end());

    ti->get_wp_set().reserve(storage.size());
    std::vector<wp_meta*> wped{};
    wped.reserve(storage.size());

    for (auto&& wp_target : storage) {
        Storage page_set_meta_storage = get_page_set_meta_storage();
        std::string_view page_set_meta_storage_view = {
                reinterpret_cast<char*>( // NOLINT
                        &page_set_meta_storage),
                sizeof(page_set_meta_storage)};
        std::string_view storage_view = {
                reinterpret_cast<char*>(&wp_target), // NOLINT
                sizeof(wp_target)};
        auto* elem_ptr = std::get<0>(yakushima::get<wp_meta*>(
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
        wp_meta* target_wp_meta = *elem_ptr;
        if (Status::OK != target_wp_meta->register_wp(valid_epoch, batch_id)) {
            cleanup_process();
            return Status::ERR_FAIL_WP;
        }
        wped.emplace_back(target_wp_meta); // for fast cleanup at failure
        ti->get_wp_set().emplace_back(wp_target);
    }

    return Status::OK;
}

} // namespace shirakami::wp