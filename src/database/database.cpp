
#include "storage.h"

#include "concurrency_control/include/wp.h"
#include "database/include/database.h"
#include "database/include/logging.h"

#include "shirakami/interface.h"

namespace shirakami {

Status database_set_logging_callback_body(log_event_callback const& callback) {
    if (callback) {
        // callback is executable
        set_log_event_callback(callback);
        return Status::OK;
    }
    return Status::WARN_INVALID_ARGS;
}

Status database_set_logging_callback(log_event_callback const& callback) {
    shirakami_log_entry << "database_set_logging_callback";
    auto ret = database_set_logging_callback_body(callback);
    shirakami_log_exit << "database_set_logging_callback";
    return ret;
}

[[maybe_unused]] Status delete_all_records() { // NOLINT
    //check list of all storage
    std::vector<Storage> storage_list;
    storage::list_storage(storage_list);

    // delete all storages.
    for (auto&& elem : storage_list) {
        if (elem != wp::get_page_set_meta_storage()) {
            if (delete_storage(elem) != Status::OK) {
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