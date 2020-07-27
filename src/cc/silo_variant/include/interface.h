/**
 * @file interface.h
 */

#pragma once

#include "thread_info.h"

namespace shirakami::silo_variant {

Status abort(Token token);  // NOLINT

Status close_scan(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                  ScanHandle handle);

Status commit(Token token);  // NOLINT

[[maybe_unused]] Status delete_all_records();  // NOLINT

Status delete_record(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                     const char* key, std::size_t len_key);

Status enter(Token& token);  // NOLINT

void fin();

Status init(std::string_view log_directory_path);  // NOLINT

Status insert(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              const char* key, std::size_t len_key, const char* val,
              std::size_t len_val);

Status leave(Token token);  // NOLINT

Status read_from_scan(Token token,  // NOLINT
                      [[maybe_unused]] Storage storage, ScanHandle handle,
                      Tuple** tuple);

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

Status open_scan(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                 const char* lkey, std::size_t len_lkey, bool l_exclusive,
                 const char* rkey, std::size_t len_rkey, bool r_exclusive,
                 ScanHandle& handle);

Status scan_key(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                const char* lkey, std::size_t len_lkey, bool l_exclusive,
                const char* rkey, std::size_t len_rkey, bool r_exclusive,
                std::vector<const Tuple*>& result);

Status scannable_total_index_size(Token token,  // NOLINT
                                  [[maybe_unused]] Storage storage,
                                  ScanHandle& handle, std::size_t& size);

Status search_key(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
                  const char* key, std::size_t len_key, Tuple** tuple);

/**
 * @brief Transaction begins.
 * @details Get an epoch accessible to this transaction.
 * @return void
 */
void tx_begin(Token token);

Status update(Token token, [[maybe_unused]] Storage sotrage,  // NOLINT
              const char* key, std::size_t len_key, const char* val,
              std::size_t len_val);

Status upsert(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              const char* key, std::size_t len_key, const char* val,
              std::size_t len_val);

void write_phase(ThreadInfo* ti, const tid_word& max_rset,
                 const tid_word& max_wset);

}  // namespace shirakami::silo_variant
