
#include "boost/filesystem/path.hpp"

#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

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
    [[maybe_unused]] limestone::api::snapshot ss;
#if 1
    // todo delete. for temporally impl.
    [[maybe_unused]] limestone::api::cursor cs{};
#else
    // todo use
    //[[maybe_unused]] limestone::api::cursor cs{ss.get_cursor()};
#endif
// comment out due to not implementation body(未定義参照)

/**
     * The cursor point the first entry at calling first next(). 
     */
#if 0
    while (cs.next()) { // the next body is none.
        [[maybe_unused]] Storage st{cs.storage()};
        std::string key{};
        std::string val{};
        cs.key(key);
        cs.value(val);
        // todo create kvs entry from these info.
    }
#endif
}

#endif

} // namespace shirakami::datastore