
#include "concurrency_control/wp/include/lpwal.h"

namespace shirakami::lpwal {

/**
  * @brief It executes log_channel_.add_entry for entire logs_.
  */
void add_entry_from_logs([[maybe_unused]] handler& handle) {
#if 0
    for (auto&& log_elem : handle.get_logs()) {
        if (log_elem.get_is_delete()) {
            // this is delete
            // todo for delete
        } else {
            // this is write
            // now no source
            //handle.get_log_channel().add_entry(
            //        static_cast<limestone::detail::storage_id_type>(
            //                log_elem.get_st()),
            //        log_elem.get_key(), log_elem.get_val(),
            //        log_elem.get_wv().get_major_write_version());
            // todo: last args fix for full write version
        }
    }

    handle.get_logs().clear();
#endif
}

void flush_log(handler& handle) {
    /**
      * It writes the implementation here because of the file dependency, 
      * but it separates it to the source file eventually.
      */
    // handle.get_log_channel().begin_session(); // now no source
    add_entry_from_logs(handle);
    // handle.get_log_channel().end_session(); // now no source
}

} // namespace shirakami::lpwal