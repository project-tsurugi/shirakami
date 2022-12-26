
#include "storage.h"

#include "shirakami/logging.h"
#include "shirakami/result_info.h"

#include "glog/logging.h"

namespace shirakami {

void result_info::set_storage_name(Storage const storage) {
    std::string out{};
    if (storage::key_handle_map_get_key(storage, out) == Status::OK) {
        set_storage_name(out);
    } else {
        LOG(INFO) << log_location_prefix
                  << "key handle map. user may use storage not existed ";
    }
}

void result_info::set_key_storage_name(std::string_view const key,
                                       Storage const storage) {
    // set storage name
    set_storage_name(storage);

    // set key
    set_key(key);
}

} // namespace shirakami