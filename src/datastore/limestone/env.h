#pragma once

#include <memory>

#include "limestone/api/datastore.h"

namespace shirakami::datastore {

inline std::unique_ptr<limestone::api::datastore> datastore_; // NOLINT

[[maybe_unused]] static limestone::api::datastore* get_datastore() {
    return datastore_.get();
}

[[maybe_unused]] static void
start_datastore(limestone::detail::configuration conf) {
    datastore_ = std::make_unique<limestone::api::datastore>(conf);
}

} // namespace shirakami::datastore