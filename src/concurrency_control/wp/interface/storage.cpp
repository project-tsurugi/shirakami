/**
 * @file wp/storage.cpp
 */

#include <cstdlib>

#include "storage.h"

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

#include "shirakami/interface.h"
#include "shirakami/logging.h"
#include "shirakami/storage_options.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

void write_storage_metadata(std::string_view key, Storage st,
                            storage_option const& options) {
    Token s{};
    while (enter(s) != Status::OK) { _mm_pause(); }
    std::string value{};
    // value = Storage + id + payload
    value.append(reinterpret_cast<char*>(&st), sizeof(st)); // NOLINT
    storage_option::id_t id = options.id();
    value.append(reinterpret_cast<char*>(&id), sizeof(id)); // NOLINT
    std::string payload{options.payload()};
    value.append(payload);
    auto ret = upsert(s, storage::meta_storage, key, value);
    if (ret != Status::OK) {
        LOG(ERROR) << "reachable path";
        return;
    }
    if (commit(s) == Status::OK) {
        leave(s);
        return;
    } // else
    LOG(ERROR) << "reachable path";
    return;
}

Status remove_storage_metadata([[maybe_unused]] Storage st,
                               [[maybe_unused]] storage_option const& options) {
    // todo impl
    return Status::ERR_NOT_IMPLEMENTED;
}

Status create_storage(std::string_view const key, Storage& storage,
                      storage_option const& options) {
    // check key existence
    Storage st{};
    if (storage::key_handle_map_get_storage(key, st) == Status::OK) {
        return Status::WARN_ALREADY_EXISTS;
    }
    // point (*1)

    auto ret = storage::create_storage(storage, options);
    if (ret != Status::OK) { return ret; }
    // success create_storage
    // point (*2)
    if (storage::key_handle_map_push_storage(key, storage) != Status::OK) {
        // interrupt between (*1) and (*2)
        storage::delete_storage(storage);
        return Status::WARN_ALREADY_EXISTS;
    }

    write_storage_metadata(key, storage, options);
    return Status::OK;
}

Status delete_storage(Storage const storage) {
    std::lock_guard<std::shared_mutex> lk{storage::get_mtx_key_handle_map()};
    auto ret = storage::delete_storage(storage);
    if (ret != Status::OK) { return ret; }
    // delete_storage was succeeded
    storage::key_handle_map_erase_storage_without_lock(storage);
    return Status::OK;
}

Status get_storage(std::string_view const key, Storage& out) {
    return storage::key_handle_map_get_storage(key, out);
}

Status list_storage(std::vector<std::string>& out) {
    return storage::list_storage(out);
}

Status storage_get_options(Storage storage, storage_option& options) {
    // key handle map get key
    std::string key{};
    auto ret = storage::key_handle_map_get_key(storage, key);
    if (ret != Status::OK) {
        // storage not found
        return ret;
    } // storage found
    Token s{};
    while (enter(s) != Status::OK) { _mm_pause(); }
    std::string value{};
    std::size_t try_num{0};
    for (;;) {
        ret = search_key(s, storage::meta_storage, key, value);
        if (ret != Status::OK) {
            LOG(ERROR) << "unreachable path: " << s << ", "
                       << storage::meta_storage << ", " << key << ", " << value
                       << ret;
            return Status::ERR_FATAL;
        }
        if (commit(s) == Status::OK) { break; } // NOLINT
        // Someone may executed storage_set_options and it occurs occ error.
        _mm_pause();
        ++try_num;
        if (try_num > 100) {
            LOG(INFO) << "strange statement";
            leave(s);
            return Status::WARN_ILLEGAL_OPERATION;
        }
    }
    leave(s);
    // value = Storage + id + payload
    storage_option::id_t id{};
    memcpy(&id, value.data() + sizeof(Storage), // NOLINT
           sizeof(storage_option::id_t));
    std::string payload{};
    if (value.size() > sizeof(Storage) + sizeof(storage_option::id_t)) {
        payload.append(value.data() + sizeof(Storage) +      // NOLINT
                               sizeof(storage_option::id_t), // NOLINT
                       value.size() - sizeof(Storage) -
                               sizeof(storage_option::id_t));
    }
    options = {id, payload};
    return Status::OK;
}

Status storage_set_options(Storage storage, storage_option const& options) {
    // key handle map get key
    std::string key{};
    auto ret = storage::key_handle_map_get_key(storage, key);
    if (ret != Status::OK) {
        // storage not found
        return ret;
    } // storage found
    Token s{};
    while (enter(s) != Status::OK) { _mm_pause(); }
    std::string value{};
    // value = Storage + id + payload
    value.append(reinterpret_cast<char*>(&storage), sizeof(storage)); // NOLINT
    storage_option::id_t id = options.id();
    value.append(reinterpret_cast<char*>(&id), sizeof(id)); // NOLINT
    std::string payload{options.payload()};
    value.append(payload);
    ret = upsert(s, storage::meta_storage, key, value);
    if (ret != Status::OK) {
        LOG(ERROR) << "invalid use";
        return Status::ERR_FATAL;
    }
    if (commit(s) == Status::OK) {
        leave(s);
        return Status::OK;
    } // else
    LOG(ERROR) << "unreachable path";
    return Status::ERR_FATAL;
}

Status storage::register_storage(Storage storage, storage_option options) {
    std::string_view storage_view = {
            reinterpret_cast<char*>(&storage), // NOLINT
            sizeof(storage)};
    auto rc = yakushima::create_storage(std::string_view(storage_view));
    // create storage must return WARN_UNIQUE_RESTRICTION or OK
    if (rc == yakushima::status::WARN_UNIQUE_RESTRICTION) { // NOLINT
        return Status::WARN_ALREADY_EXISTS;
    }
    if (rc != yakushima::status::OK) {
        LOG(ERROR) << "programming error.";
        return Status::ERR_FATAL;
    }

    if (wp::get_initialized()) {
        yakushima::Token ytoken{};
        while (yakushima::enter(ytoken) != yakushima::status::OK) {
            _mm_pause();
        }
        Storage page_set_meta_storage = wp::get_page_set_meta_storage();
        wp::page_set_meta* page_set_meta_ptr{
                new wp::page_set_meta(std::move(options))};
        auto rc = yakushima::put<wp::page_set_meta*>(
                ytoken,
                {reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
                 sizeof(page_set_meta_storage)},
                storage_view, &page_set_meta_ptr,
                sizeof(page_set_meta_ptr)); // NOLINT
        if (yakushima::status::OK != rc) {
            LOG(ERROR) << rc;
            return Status::ERR_FATAL_INDEX;
        }
        rc = yakushima::leave(ytoken);
        if (yakushima::status::OK != rc) {
            LOG(ERROR) << rc;
            return Status::ERR_FATAL_INDEX;
        }
    }

    return Status::OK;
}

Status storage::create_storage(Storage& storage,
                               storage_option const& options) {
    // compute storage id.
    Storage storage_id = options.id();
    if (storage_id == storage_id_undefined) {
        // storage id is not specified by shirakami-user.
        get_new_storage_num(storage);
        // check depletion
        if ((storage >> 32) > 0) { // NOLINT
            VLOG(log_trace)
                    << "system defined storage id depletion. you should "
                       "implement re-using storage id.";
            return Status::WARN_STORAGE_ID_DEPLETION;
        }
        // higher bit is used for system defined.
        storage <<= 32; // NOLINT
    } else {
        // storage id is specified by shirakami-user.
        storage = storage_id;
        // check depletion
        if ((storage >> 32) > 0) { // NOLINT
            VLOG(log_trace) << "user defined storage id depletion. you should "
                               "implement re-using storage id.";
            return Status::WARN_STORAGE_ID_DEPLETION;
        }
    }

    // create storage using id computed.
    return register_storage(storage, options);
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

    if (!wp::get_finalizing() && storage != storage::meta_storage &&
        storage != storage::sequence_storage) {
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
            LOG(ERROR) << "missing error" << std::endl
                       << " " << page_set_meta_storage << " " << storage
                       << std::endl;
            return Status::ERR_FATAL;
        }
        delete *out.first; // NOLINT
        rc = yakushima::remove(
                ytoken,
                {reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
                 sizeof(page_set_meta_storage)},
                storage_view);
        if (yakushima::status::OK != rc) {
            LOG(ERROR) << rc;
            return Status::ERR_FATAL;
        }
        rc = yakushima::leave(ytoken);
        if (yakushima::status::OK != rc) {
            LOG(ERROR) << rc;
            return Status::ERR_FATAL;
        }
    }
    auto rc = yakushima::delete_storage(storage_view);
    if (yakushima::status::OK != rc) { // NOLINT
        LOG(ERROR) << rc;
        return Status::ERR_FATAL;
    }

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
        if (dest != storage::wp_meta_storage && dest != storage::meta_storage &&
            dest != storage::sequence_storage) {
            out.emplace_back(dest);
        }
    }
    return Status::OK;
}

void storage::get_new_storage_num(Storage& storage) {
    storage = strg_ctr_.fetch_add(1);
}

void storage::init() { storage::set_strg_ctr(storage::initial_strg_ctr); }

void storage::init_meta_storage() {
    auto ret = storage::register_storage(storage::meta_storage);
    if (ret != Status::OK) { LOG(ERROR) << "programming error"; }
    ret = storage::register_storage(storage::sequence_storage);
    if (ret != Status::OK) { LOG(ERROR) << "programming error"; }
}

void storage::fin() {
    // clear meta storage
    auto ret = storage::delete_storage(storage::meta_storage);
    if (ret != Status::OK) { LOG(ERROR) << "programming error"; }
    ret = storage::delete_storage(storage::sequence_storage);
    if (ret != Status::OK) { LOG(ERROR) << "programming error"; }

    // clear key storage map
    storage::key_handle_map_clear();
}

} // namespace shirakami