#pragma once

#include <string_view>
#include <vector>

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"

#include "shirakami/scheme.h"

namespace shirakami::long_tx {

extern Status abort(session* ti);

extern Status check_commit(Token token);

extern Status commit(session* ti);

extern Status search_key(session* ti, Storage storage, std::string_view key,
                         std::string& value, bool read_value = true); // NOLINT

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

/**
 * @brief
 *
 * @param[in] ti
 * @param[in] wp_meta_ptr
 * @param[in] read_keyread information about key.
 */
[[maybe_unused]] extern void
wp_verify_and_forwarding(session* ti, wp::wp_meta* wp_meta_ptr,
                         std::string_view read_key);

[[maybe_unused]] extern void wp_verify_and_forwarding(session* ti,
                                                      wp::wp_meta* wp_meta_ptr);

[[maybe_unused]] extern void update_local_read_range(session* ti,
                                                     wp::wp_meta* wp_meta_ptr,
                                                     std::string_view key);

// for ltx scan
[[maybe_unused]] extern void update_local_read_range(session* ti,
                                                     wp::wp_meta* wp_meta_ptr,
                                                     std::string_view l_key,
                                                     scan_endpoint l_end);

[[maybe_unused]] extern void
update_local_read_range(session* ti, wp::wp_meta* wp_meta_ptr,
                        std::string_view l_key, scan_endpoint l_end,
                        std::string_view r_key, scan_endpoint r_end);

} // namespace shirakami::long_tx
