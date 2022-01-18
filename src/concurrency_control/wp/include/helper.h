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
 * @param[in] read_value whether read the value.
 */
Status read_record(Record* rec_ptr, tid_word& tid, std::string& val, bool read_value);

} // namespace shirakami