
#include "concurrency_control/wp/include/lpwal.h"

namespace shirakami::lpwal {

/**
  * @brief It executes log_channel_.add_entry for entire logs_.
  */
void add_entry_from_logs([[maybe_unused]] handler& handle) {
#if 1
    for (auto&& log_elem : handle.get_logs()) {
        if (log_elem.get_is_delete()) {
            // this is delete
            // todo for delete
        } else {
            // this is write
            // now no source
            handle.get_log_channel_ptr()->add_entry(
                    static_cast<limestone::api::storage_id_type>(
                            log_elem.get_st()),
                    log_elem.get_key(), log_elem.get_val(),
                    limestone::api::write_version_type());
            // todo: last args fix for full write version
        }
    }

    handle.get_logs().clear();
#endif
}

void flush_log(handler& handle) {
    handle.get_log_channel_ptr()->begin_session();
    add_entry_from_logs(handle);
    handle.get_log_channel_ptr()->end_session();
}

} // namespace shirakami::lpwal