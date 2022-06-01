
#include "boost/filesystem/path.hpp"

#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/scheme.h"

#include "datastore/limestone/include/datastore.h"

namespace shirakami::datastore {

#if defined(PWAL)

void init_about_session_table(std::string_view log_dir_path) {
    boost::filesystem::path log_dir{std::string(log_dir_path)};
    for (auto&& elem : session_table::get_session_table()) {
        elem.get_lpwal_handle().set_log_channel_ptr(
                &get_datastore()->create_channel(log_dir));
    }
}

void recovery_from_datastore() {
    [[maybe_unused]] limestone::api::snapshot* ss{get_datastore()->get_snapshot()};
    [[maybe_unused]] limestone::api::cursor cs{ss->get_cursor()};

    /**
     * The cursor point the first entry at calling first next(). 
     */
    yakushima::Token tk{};
    if (yakushima::enter(tk) != yakushima::status::OK) {
        LOG(ERROR) << "programming error";
    }
    while (cs.next()) { // the next body is none.
        [[maybe_unused]] Storage st{cs.storage()};
        std::string key{};
        std::string val{};
        cs.key(key);
        cs.value(val);
        // create kvs entry from these info.
        if (yakushima::status::OK != put<Record>(tk, st, key, val)) {
            LOG(ERROR) << "not unique. to discuss or programming error.";
        }
    }
    if (yakushima::leave(tk) != yakushima::status::OK) {
        LOG(ERROR) << "programming error";
    }
}

#endif

} // namespace shirakami::datastore
