
#include <algorithm>
#include <string_view>
#include <vector>

#include "storage.h"

#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/include/tuple_local.h"

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

Status find_read_by(Storage const st, read_by_bt*& ret) {
    page_set_meta* psm{};
    auto rc{find_page_set_meta(st, psm)};
    if (rc == Status::WARN_NOT_FOUND) { return rc; }
    ret = psm->get_read_by_ptr();
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

    if (auto rc{storage::create_storage(storage::wp_meta_storage)};
        rc != Status::OK) {
        LOG(FATAL) << rc;
    }
    set_page_set_meta_storage(storage::wp_meta_storage);
    set_initialized(true);
    return Status::OK;
}

Status write_preserve(Token token, std::vector<Storage> storage,
                      std::size_t batch_id, epoch::epoch_t valid_epoch) {
    // decide storage form
    auto* ti = static_cast<session*>(token);
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
        std::pair<page_set_meta**, std::size_t> out{};
        auto rc{yakushima::get<page_set_meta*>(page_set_meta_storage_view,
                                               storage_view, out)};

        auto cleanup_process = [ti, &wped, batch_id]() {
            for (auto&& elem : wped) {
                if (Status::OK != elem->remove_wp(batch_id)) {
                    LOG(FATAL) << "vanish registered wp.";
                    std::abort();
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