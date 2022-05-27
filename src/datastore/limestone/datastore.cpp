
#include "boost/filesystem/path.hpp"

#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

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
    //[[maybe_unused]] limestone::api::cursor cs{ss.get_cursor()};
    // comment out due to not implementation body(未定義参照)

    /**
     * todo
     * cursor implementation is not yet
     * cursor から storage, key, value を引き出し、shirakami KVS に
     * 格納し、next() でカーソルを進めることをク繰り返す。
     */
}

#endif

} // namespace shirakami::datastore