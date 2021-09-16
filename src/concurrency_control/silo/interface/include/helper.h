/**
 * @file concurrency_control/silo/interface/include/helper.h
 */

#pragma once

#include "concurrency_control/silo/include/session.h"

namespace shirakami {

/**
 * @brief read record by using dest given by caller and store read info to res
 * given by caller.
 * @pre the dest wasn't already read by itself.
 * @param [out] res it is stored read info.
 * @param [in] dest read record pointed by this dest.
 * @return WARN_CONCURRENT_DELETE No corresponding record in masstree. If you
 * have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::OK, it was ended correctly.
 * but it isn't committed yet.
 */
Status read_record(Record &res, const Record* dest);  // NOLINT

void write_phase(session* ti, const tid_word &max_r_set, const tid_word &max_w_set, commit_property cp);

}  // namespace shirakami
