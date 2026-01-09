#pragma once

#include <memory>
#include <string_view>

#include "limestone/api/datastore.h"

namespace shirakami::datastore {

inline limestone::api::datastore* datastore_; // NOLINT

// whether shirakami creates a datastore and owns it
//  * false: normal, using datastore that passed by init()
//  * true: for compat with old code, or testing
inline bool own_datastore_{false}; // NOLINT

[[maybe_unused]] static limestone::api::datastore* get_datastore() {
    return datastore_;
}

[[maybe_unused]] static void
create_datastore(limestone::api::configuration const& conf) {
    datastore_ = new limestone::api::datastore(conf); // NOLINT
    own_datastore_ = true;
}

[[maybe_unused]] static void set_datastore(limestone::api::datastore* datastore) {
    datastore_ = datastore;
    own_datastore_ = false;
}

[[maybe_unused]] static void release_datastore() {
    if (own_datastore_) {
        delete datastore_; // NOLINT
        own_datastore_ = false;
    }
    datastore_ = nullptr;
}

[[maybe_unused]] [[nodiscard]] static bool get_own_datastore() {
    return own_datastore_;
}

/**
 * @brief It executes create_channel and pass it to shirakami's executor.
 */
#ifdef HAVE_LIMESTONE_DATASTORE_CREATE_CHANNEL_NONE
void init_about_session_table();
#else
void init_about_session_table(std::string_view log_dir_path);
#endif

/**
 * @brief recovery from datastore
 * @details This is called at shirakami::init command.
 * @pre It has finished recovery of datastore.
 */
void recovery_from_datastore(std::size_t thread_num);

/**
 * @brief scan for all and logging that.
 */
void scan_all_and_logging();

} // namespace shirakami::datastore
