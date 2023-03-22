/**
 * @file concurrency_control/include/helper.h
 */

#pragma once

#include "record.h"
#include "tid.h"
#include "wp.h"

namespace shirakami {

/**
 * @brief It is used by short tx's occ read
 * 
 * @param rec_ptr 
 * @param tid 
 * @param val 
 * @param read_value 
 * @return Status 
 */
Status read_record(Record* rec_ptr, tid_word& tid, std::string& val,
                   bool read_value = true); // NOLINT

} // namespace shirakami