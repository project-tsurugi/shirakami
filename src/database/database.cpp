
#include "database/include/database.h"

#include "shirakami/interface.h"

namespace shirakami {

Status database_set_logging_callback(log_event_callback callback) {
    if (callback) {
        // callback is executable
        set_log_event_callback(callback);
        return Status::OK;
    }
    return Status::WARN_INVALID_ARGS;
}

} // namespace shirakami