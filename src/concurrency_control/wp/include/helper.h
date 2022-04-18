/**
 * @file concurrency_control/wp/include/helper.h
 */

#pragma once

#include "record.h"
#include "tid.h"
#include "wp.h"

namespace shirakami {

Status read_record(Record* rec_ptr, tid_word& tid, std::string& val, 
                   bool read_value = true); // NOLINT

} // namespace shirakami