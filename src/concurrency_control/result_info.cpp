
#include "storage.h"

#include "shirakami/logging.h"
#include "shirakami/result_info.h"

#include "glog/logging.h"

namespace shirakami {

void result_info::set_key_storage_name(std::string_view key, Storage storage) {
    std::string out{};
    if (storage::key_handle_map_get_key(storage, out) == Status::OK) {
        set_storage_name(out);
    } else {
        LOG(ERROR) << log_location_prefix
                   << "key handle map error. user may cause "
                      "undefined behavior";
    }
    set_key(key);
}

} // namespace shirakami