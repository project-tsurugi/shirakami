#pragma once

#include <string_view>
#include <vector>

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/scheme.h"
#include "shirakami/tuple.h"

namespace shirakami::long_tx {

extern Status abort(session* ti);

extern Status commit(session* ti, commit_param* cp);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         std::string& value, bool read_value = true); // NOLINT

extern Status tx_begin(session* ti, std::vector<Storage> write_preserve);

/**
 * @brief version function for long tx.
 * @param[in] rec pointer to record.
 * @param[in] ep long tx's epoch.
 * @param[out] ver the target version.
 * @return Status::OK success.
 * @return Status::ERR_FATAL programming error.
 */
extern Status version_function(Record* rec, epoch::epoch_t ep, version*& ver);

} // namespace shirakami::long_tx