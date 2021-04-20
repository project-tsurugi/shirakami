/**
 * @file storage.cpp
 */

#include "storage.h"

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

    if (yakushima::create_storage(std::string_view(reinterpret_cast<char*>(&storage), sizeof(storage))) != yakushima::status::OK) { // NOLINT
        return Status::WARN_INVARIANT;
    }

    return Status::OK;
}

Status storage::exist_storage(Storage storage) {
    auto ret = yakushima::find_storage({reinterpret_cast<char*>(&storage), sizeof(storage)}); // NOLINT
    if (ret == yakushima::status::OK) return Status::OK;
    return Status::WARN_NOT_FOUND;
}

Status storage::delete_storage(Storage storage) {
    auto ret = yakushima::delete_storage({reinterpret_cast<char*>(&storage), sizeof(storage)}); // NOLINT
    if (ret == yakushima::status::OK) return Status::OK;
    return Status::WARN_INVALID_HANDLE;
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

} // namespace shirakami