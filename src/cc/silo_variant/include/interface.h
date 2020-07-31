/**
 * @file src/cc/silo_variant/include/interface.h
 */

#pragma once

#include "thread_info.h"

namespace shirakami::cc_silo_variant {

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
Status read_record(Record& res, const Record* dest);  // NOLINT

/**
 * @brief Transaction begins.
 * @details Get an epoch accessible to this transaction.
 * @return void
 */
void tx_begin(Token token);

void write_phase(ThreadInfo* ti, const tid_word& max_rset,
                 const tid_word& max_wset);

}  // namespace shirakami::silo_variant
