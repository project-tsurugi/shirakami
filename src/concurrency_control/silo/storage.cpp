/**
 * @file silo/storage.cpp
 */

#include "storage.h"

#ifdef WP

#include "concurrency_control/wp/include/record.h"

#else

#include "concurrency_control/silo/include/record.h"
#include "concurrency_control/silo/include/session_table.h"

#endif

#ifdef CPR

#include "fault_tolerance/include/cpr.h"

#endif

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

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

Status storage::register_storage(Storage& storage) {
    get_new_storage_num(storage);

    if (yakushima::create_storage(std::string_view(
                reinterpret_cast<char*>(&storage), sizeof(storage))) !=
        yakushima::status::OK) { // NOLINT
        return Status::WARN_INVARIANT;
    }

    return Status::OK;
}

Status storage::exist_storage(Storage storage) {
    auto ret = yakushima::find_storage(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}); // NOLINT
    if (ret == yakushima::status::OK) return Status::OK;
    return Status::WARN_NOT_FOUND;
}

Status storage::delete_storage(Storage storage) { // NOLINT
    auto ret = yakushima::find_storage(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}); // NOLINT
    if (ret != yakushima::status::OK) return Status::WARN_INVALID_HANDLE;
    // exist storage

    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    constexpr std::size_t v_index{1};
    yakushima::scan({reinterpret_cast<char*>(&storage), sizeof(storage)}, "",
                    yakushima::scan_endpoint::INF, "",
                    yakushima::scan_endpoint::INF, scan_res); // NOLINT

    if (scan_res.size() < std::thread::hardware_concurrency() * 10) { // NOLINT
        // single thread clean up
        for (auto&& itr : scan_res) {
            delete *std::get<v_index>(itr); // NOLINT
        }
    } else {
        // multi threads clean up
        auto process = [&scan_res]([[maybe_unused]] std::size_t const begin,
                                   [[maybe_unused]] std::size_t const end) {
            for (std::size_t i = begin; i < end; ++i) {
                delete *std::get<v_index>(scan_res[i]); // NOLINT
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
        for (auto&& th : th_vc) th.join();
    }

    yakushima::delete_storage(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}); // NOLINT
    std::unique_lock lock{storage::get_mt_reuse_num()};
    storage::get_reuse_num().emplace_back(storage);
    return Status::OK;
}

Status storage::list_storage(std::vector<Storage>& out) {
    std::vector<std::pair<std::string, yakushima::tree_instance*>> rec;
    yakushima::list_storages(rec);
    if (rec.empty()) return Status::WARN_NOT_FOUND;
    out.clear();
    for (auto&& elem : rec) {
        //Due to invariants, the type is known by the developer.
        Storage dest{};
        memcpy(&dest, elem.first.data(), sizeof(dest));
        out.emplace_back(dest);
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