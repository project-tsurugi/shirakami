
#include "storage.h"

#include "concurrency_control/include/wp.h"

#include "database/include/database.h"

#include "shirakami/interface.h"

namespace shirakami {

Status database_set_logging_callback(log_event_callback const& callback) {
    if (callback) {
        // callback is executable
        set_log_event_callback(callback);
        return Status::OK;
    }
    return Status::WARN_INVALID_ARGS;
}

[[maybe_unused]] Status delete_all_records() { // NOLINT
    //check list of all storage
    std::vector<Storage> storage_list;
    storage::list_storage(storage_list);

    // delete all storages.
    for (auto&& elem : storage_list) {
        if (elem != wp::get_page_set_meta_storage()) {
            if (delete_storage(elem, false) != Status::OK) {
                LOG(ERROR) << log_location_prefix << "try delete_storage("
                           << elem << ")";
                return Status::ERR_FATAL;
            }
        }
    }

    storage::key_handle_map_clear();

    return Status::OK;
}

} // namespace shirakami