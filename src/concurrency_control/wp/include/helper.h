#pragma once

#include "record.h"
#include "tid.h"
#include "wp.h"

namespace shirakami {

/**
 * @brief Read record by using arguments infomation. It is used for optimistic
 * read.
 * @param[in] rec_ptr Target of read
 * @param[out] tid The result timestamp of read. It will be used for validation.
 * @param[out] val The result value of read.
 */
Status read_record(Record* rec_ptr, tid_word& tid, std::string*& val);

/**
 * @brief There is no metadata that should be there.
 */
[[maybe_unused]] extern wp::wp_meta::wped_type find_wp(Storage storage);

} // namespace shirakami