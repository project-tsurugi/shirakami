#pragma once

#include <string_view>
#include <vector>

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/scheme.h"
#include "shirakami/tuple.h"

namespace shirakami::long_tx {

extern Status abort(session* ti);

extern Status change_wp_epoch(session* ti, epoch::epoch_t target);

extern Status commit(session* ti, commit_param* cp);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         std::string& value, bool read_value = true); // NOLINT

extern Status tx_begin(session* ti, std::vector<Storage> write_preserve);

/**
 * @brief version function for long tx.
 * @param[in] rec pointer to record.
 * @param[in] ep long tx's epoch.
 * @param[out] ver the target version.
 * @param[out] is_latest @a ver is 
 * @param[out] f_check if this function select latest version, it is the 
 * tid_word when optimistic selection.
 * @return Status::OK success.
 * @return Status::WARN_NOT_FOUND the target version is not found.
 * @return Status::ERR_FATAL programming error.
 */
extern Status version_function_with_optimistic_check(Record* rec,
                                                     epoch::epoch_t ep,
                                                     version*& ver,
                                                     bool& is_latest,
                                                     tid_word& f_check);

/**
 * @brief version function for long tx.
 * @param[in] ep long tx's epoch.
 * @param[in,out] ver in: the start point version of version traverse. out: the 
 * target version to read.
 * @return Status::OK success.
 * @return Status::WARN_NOT_FOUND the target version is not found.
 * @return Status::ERR_FATAL programming error.
 */
extern Status version_function_without_optimistic_check(epoch::epoch_t ep,
                                                        version*& ver);

extern Status version_traverse_and_read(session* ti, Record* rec_ptr,
                                        std::string& value, bool read_value);

} // namespace shirakami::long_tx