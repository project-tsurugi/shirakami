
#include <glog/logging.h>
#include <ostream>
#include <string_view>
#include <vector>

#include "storage.h"
#include "concurrency_control/include/wp.h"
#include "database/include/database.h"
#include "shirakami/api_storage.h"
#include "shirakami/logging.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"

namespace shirakami {

[[maybe_unused]] Status delete_all_records() { // NOLINT
    //check list of all storage
    std::vector<Storage> storage_list;
    storage::list_storage(storage_list);

    // delete all storages.
    for (auto&& elem : storage_list) {
        if (elem != wp::get_page_set_meta_storage()) {
            if (delete_storage(elem) != Status::OK) {
                LOG_FIRST_N(ERROR, 1) << log_location_prefix
                                      << "try delete_storage(" << elem << ")";
                return Status::ERR_FATAL;
            }
        }
    }

    storage::key_handle_map_clear();

    return Status::OK;
}

} // namespace shirakami
