#include <algorithm>

#include "boost/filesystem/path.hpp"

#include "storage.h"

#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"
#include "shirakami/scheme.h"

#include "datastore/limestone/include/datastore.h"
#include "datastore/limestone/include/limestone_api_helper.h"

#include "glog/logging.h"

namespace shirakami::datastore {

#if defined(PWAL)

void init_about_session_table(std::string_view log_dir_path) {
    boost::filesystem::path log_dir{std::string(log_dir_path)};
    for (auto&& elem : session_table::get_session_table()) {
        elem.get_lpwal_handle().set_log_channel_ptr(
                create_channel(get_datastore(), log_dir));
    }
}

void recovery_storage_meta(std::vector<Storage>& st_list) {
    std::sort(st_list.begin(), st_list.end());
    st_list.erase(std::unique(st_list.begin(), st_list.end()), st_list.end());
    if (st_list.back() >= (storage::initial_strg_ctr << 32)) { // NOLINT
        storage::set_strg_ctr((st_list.back() >> 32) + 1);     // NOLINT
    } else {
        storage::set_strg_ctr(storage::initial_strg_ctr);
    }
}

void recovery_from_datastore() {
    auto ss = get_snapshot(get_datastore());

    /**
     * The cursor point the first entry at calling first next(). 
     */
    yakushima::Token tk{};
    if (yakushima::enter(tk) != yakushima::status::OK) {
        LOG(ERROR) << "programming error";
    }
    std::vector<Storage> st_list{};

    auto cursor = ss->get_cursor();
    while (cursor->next()) { // the next body is none.
        Storage st{cursor->storage()};
        std::string key{};
        std::string val{};
        cursor->key(key);
        cursor->value(val);
        // check storage exist
        if (st != storage::meta_storage) {
            shirakami::storage::register_storage(st);
            st_list.emplace_back(st);
            // create kvs entry from these info.
            if (yakushima::status::OK != put<Record>(tk, st, key, val)) {
                LOG(ERROR) << "not unique. to discuss or programming error.";
            }
        } else {
            // recovery storage. The storage may have not been operated.
            Storage st2{};
            if (val.size() != sizeof(st2)) {
                LOG(ERROR) << "programming error";
            }
            memcpy(&st2, val.data(), sizeof(st2));
            shirakami::storage::register_storage(st2);
            // the storage may be already created by log_entry
            storage::key_handle_map_push_storage(key, st2);
            st_list.emplace_back(st2);
        }
    }
    if (!st_list.empty()) {
        // recovery storage meta
        recovery_storage_meta(st_list);
    }
    if (yakushima::leave(tk) != yakushima::status::OK) {
        LOG(ERROR) << "programming error";
    }
}

void scan_all_and_logging() {
    // check all storage list
    std::vector<Storage> st_list{};
    storage::list_storage(st_list);

    // logging for all storage
    for (auto&& each_st : st_list) {
        // scan for index
        std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
        std::vector<std::pair<yakushima::node_version64_body,
                              yakushima::node_version64*>>
                nvec;
        auto rc = scan(each_st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                       0, scan_res, &nvec);
        if (rc == Status::OK) {
            // It found some records
            for (auto&& each_rec : scan_res) {
                Record* rec_ptr{*std::get<1>(each_rec)};
                // get key val info.
                std::string key{};
                rec_ptr->get_key(key);
                std::string val{};
                rec_ptr->get_value(val);
            }
        }
    }
}

#endif

} // namespace shirakami::datastore
