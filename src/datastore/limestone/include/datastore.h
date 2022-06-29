#pragma once

#include <memory>
#include <string_view>

#include "limestone/api/datastore.h"

namespace shirakami::datastore {

inline std::unique_ptr<limestone::api::datastore> datastore_; // NOLINT

[[maybe_unused]] static limestone::api::datastore* get_datastore() {
    return datastore_.get();
}

[[maybe_unused]] static void
start_datastore(limestone::api::configuration const& conf) {
    datastore_ = std::make_unique<limestone::api::datastore>(conf);
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
void recovery_from_datastore();

} // namespace shirakami::datastore
