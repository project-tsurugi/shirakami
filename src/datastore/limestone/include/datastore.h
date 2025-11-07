#pragma once

#include <memory>
#include <string_view>

#include "limestone/api/datastore.h"

namespace shirakami::datastore {

inline limestone::api::datastore* datastore_; // NOLINT

inline bool own_datastore_{false}; // NOLINT

[[maybe_unused]] static limestone::api::datastore* get_datastore() {
    return datastore_;
}

// for compat (own my limestone datastore)
[[maybe_unused]] static void
start_datastore(limestone::api::configuration const& conf) { // should not be "start_". change to "create_" or "setup_"
    datastore_ = new limestone::api::datastore(conf); // NOLINT
}

[[maybe_unused]] static void set_datastore(limestone::api::datastore* datastore) {
    datastore_ = datastore;
}

[[maybe_unused]] static void release_datastore() {
    if (own_datastore_) {
        delete datastore_; // NOLINT
    }
    datastore_ = nullptr;
}

[[maybe_unused]] static bool get_own_datastore() {
    return own_datastore_;
}

[[maybe_unused]] static void set_own_datastore(bool const tf) {
    own_datastore_ = tf;
}

/**
 * @brief It executes create_channel and pass it to shirakami's executor.
 */
void init_about_session_table(std::string_view log_dir_path);

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
