
#include "boost/filesystem/path.hpp"

#include "storage.h"

#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"
#include "shirakami/scheme.h"

#include "datastore/limestone/include/datastore.h"

#include "glog/logging.h"

namespace shirakami::datastore {

#if defined(PWAL)

void init_about_session_table(std::string_view log_dir_path) {
    boost::filesystem::path log_dir{std::string(log_dir_path)};
    for (auto&& elem : session_table::get_session_table()) {
        elem.get_lpwal_handle().set_log_channel_ptr(
                &get_datastore()->create_channel(log_dir));
    }
}

void recovery_storage_meta(std::vector<Storage>& st_list) {
    std::sort(st_list.begin(), st_list.end());
    st_list.erase(std::unique(st_list.begin(), st_list.end()), st_list.end());
    for (Storage st_itr = storage::initial_strg_ctr; st_itr != st_list.back();
         ++st_itr) {
        // find
        bool found{false};
        for (auto&& elem : st_list) {
            if (st_itr == elem) {
                found = true;
                break;
            }
        }
        if (!found) { storage::get_reuse_num().emplace_back(st_itr); }
    }
    storage::set_strg_ctr(st_list.back() + 1);
}

void recovery_from_datastore() {
    limestone::api::snapshot* ss(get_datastore()->get_snapshot());

    /**
     * The cursor point the first entry at calling first next(). 
     */
    yakushima::Token tk{};
    if (yakushima::enter(tk) != yakushima::status::OK) {
        LOG(ERROR) << "programming error";
    }
    std::vector<Storage> st_list{};
    LOG(INFO);
    while (ss->get_cursor().next()) { // the next body is none.
        Storage st{ss->get_cursor().storage()};
        std::string key{};
        std::string val{};
        ss->get_cursor().key(key);
        ss->get_cursor().value(val);
        // check storage exist
        storage::create_storage(st);
        LOG(INFO) << st << " : " << key << " : " << val;
        st_list.emplace_back(st);
        // create kvs entry from these info.
        if (yakushima::status::OK != put<Record>(tk, st, key, val)) {
            LOG(ERROR) << "not unique. to discuss or programming error.";
        }
    }
    LOG(INFO);
    if (!st_list.empty()) {
        // recovery storage meta
        recovery_storage_meta(st_list);
    }
    if (yakushima::leave(tk) != yakushima::status::OK) {
        LOG(ERROR) << "programming error";
    }
}

#endif

} // namespace shirakami::datastore
