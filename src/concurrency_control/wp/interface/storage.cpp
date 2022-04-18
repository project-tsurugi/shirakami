/**
 * @file wp/storage.cpp
 */

#include <cstdlib>

#include "storage.h"

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

Status register_storage(Storage& storage) {
    return storage::register_storage(storage);
}

Status exist_storage(Storage storage) {
    return storage::exist_storage(storage);
}

Status delete_storage(Storage storage) {
    return storage::delete_storage(storage);
}

Status list_storage(std::vector<Storage>& out) {
    return storage::list_storage(out);
}

Status storage::create_storage(Storage storage) {
    std::string_view storage_view = {
            reinterpret_cast<char*>(&storage), // NOLINT
            sizeof(storage)};
    if (yakushima::create_storage(std::string_view(storage_view)) !=
        yakushima::status::OK) { // NOLINT
        return Status::WARN_INVARIANT;
    }

    if (wp::get_initialized()) {
        yakushima::Token ytoken{};
        while (yakushima::enter(ytoken) != yakushima::status::OK) {
            _mm_pause();
        }
        Storage page_set_meta_storage = wp::get_page_set_meta_storage();
        wp::page_set_meta* page_set_meta_ptr{new wp::page_set_meta()};
        auto rc = yakushima::put<wp::page_set_meta*>(
                ytoken,
                {reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
                 sizeof(page_set_meta_storage)},
                storage_view, &page_set_meta_ptr,
                sizeof(page_set_meta_ptr)); // NOLINT
        if (yakushima::status::OK != rc) { LOG(FATAL) << rc; }
        yakushima::leave(ytoken);
    }

    return Status::OK;
}

Status storage::register_storage(Storage& storage) {
    get_new_storage_num(storage);
    return create_storage(storage);
}

Status storage::exist_storage(Storage storage) {
    auto ret = yakushima::find_storage(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}); // NOLINT
    if (ret == yakushima::status::OK) { return Status::OK; }
    return Status::WARN_NOT_FOUND;
}

Status storage::delete_storage(Storage storage) {
    // NOLINT
    std::unique_lock lk{garbage::get_mtx_cleaner()};

    std::string_view storage_view = {
            reinterpret_cast<char*>(&storage), // NOLINT
            sizeof(storage)};
    auto ret = yakushima::find_storage(storage_view);
    if ((ret != yakushima::status::OK) ||
        (!wp::get_finalizing() && storage == wp::get_page_set_meta_storage())) {
        return Status::WARN_INVALID_HANDLE;
    }
    // exist storage

    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    constexpr std::size_t v_index{1};
    yakushima::scan(storage_view, "", yakushima::scan_endpoint::INF, "",
                    yakushima::scan_endpoint::INF, scan_res);

    if (scan_res.size() < std::thread::hardware_concurrency() * 10) { // NOLINT
        // single thread clean up
        for (auto&& itr : scan_res) {
            if (wp::get_finalizing()) {
                delete *reinterpret_cast<wp::page_set_meta**>( // NOLINT
                        std::get<v_index>(itr));
            } else {
                Record* target_rec{*std::get<v_index>(itr)};
                delete target_rec; // NOLINT
            }
        }
    } else {
        // multi threads clean up
        auto process = [&scan_res](std::size_t const begin,
                                   std::size_t const end) {
            for (std::size_t i = begin; i < end; ++i) {
                if (wp::get_finalizing()) {
                    delete *reinterpret_cast<wp::page_set_meta**>( // NOLINT
                            std::get<v_index>(scan_res[i]));
                } else {
                    Record* target_rec{*std::get<v_index>(scan_res[i])};
                    delete target_rec; // NOLINT
                }
            }
        };
        std::size_t th_size = std::thread::hardware_concurrency();
        std::vector<std::thread> th_vc;
        th_vc.reserve(th_size);
        for (std::size_t i = 0; i < th_size; ++i) {
            th_vc.emplace_back(process, i * (scan_res.size() / th_size),
                               i != th_size - 1
                                       ? (i + 1) * (scan_res.size() / th_size)
                                       : scan_res.size());
        }
        for (auto&& th : th_vc) { th.join(); }
    }

    if (!wp::get_finalizing()) {
        Storage page_set_meta_storage = wp::get_page_set_meta_storage();
        yakushima::Token ytoken{};
        while (yakushima::enter(ytoken) != yakushima::status::OK) {
            _mm_pause();
        }
        std::pair<wp::page_set_meta**, std::size_t> out{};
        auto rc{yakushima::get<wp::page_set_meta*>(
                {reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
                 sizeof(page_set_meta_storage)},
                storage_view, out)};
        if (rc != yakushima::status::OK) {
            LOG(FATAL) << "missing error" << std::endl
                       << " " << page_set_meta_storage << " " << storage
                       << std::endl;
        }
        delete *out.first; // NOLINT
        if (yakushima::status::OK !=
            yakushima::remove(
                    ytoken,
                    {reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
                     sizeof(page_set_meta_storage)},
                    storage_view)) {
            LOG(FATAL) << "missing error";
        }
        if (yakushima::status::OK != yakushima::leave(ytoken)) {
            LOG(FATAL) << "missing error";
        }
    }
    if (yakushima::status::OK !=
        yakushima::delete_storage(storage_view)) { // NOLINT
        LOG(FATAL) << "missing error";
    }

    storage::register_reuse_num(storage);
    return Status::OK;
}

Status storage::list_storage(std::vector<Storage>& out) {
    std::vector<std::pair<std::string, yakushima::tree_instance*>> rec;
    yakushima::list_storages(rec);
    if (rec.empty()) {
        LOG(ERROR) << "There must be wp meta storage at least.";
        return Status::ERR_FATAL;
    }
    out.clear();
    for (auto&& elem : rec) {
        //Due to invariants, the type is known by the developer.
        Storage dest{};
        memcpy(&dest, elem.first.data(), sizeof(dest));
        if (dest != storage::wp_meta_storage) { out.emplace_back(dest); }
    }
    return Status::OK;
}

void storage::get_new_storage_num(Storage& storage) {
    std::unique_lock lock{storage::get_mt_reuse_num()};

    auto& storage_reuse = storage::get_reuse_num();
    if (!storage_reuse.empty()) {
        storage = storage_reuse.back();
        storage_reuse.pop_back();
    } else {
        storage = strg_ctr_.fetch_add(1);
    }
}

void storage::init() {
    storage::get_reuse_num() = {};
    storage::set_strg_ctr(storage::initial_strg_ctr);
}

} // namespace shirakami