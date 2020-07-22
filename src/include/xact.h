
/**
 * @file
 * @brief private transaction engine interface
 */

#pragma once

#include "kvs/scheme.h"
#include "masstree_beta_wrapper.h"
#include "scheme_local.h"
#include "thread_info.h"

namespace shirakami {

extern MasstreeWrapper<Record> MTDB;

/**
 * @brief find record from masstree by using args informations.
 * @return the found record pointer.
 */
extern Record* find_record_from_masstree(char const* key,  // NOLINT
                                         std::size_t len_key);

/**
 * @brief insert record to masstree by using args informations.
 * @pre the record which has the same key as the key of args have never been
 * inserted.
 * @param key
 * @param len_key
 * @param record It inserts this pointer to masstree database.
 * @return WARN_ALREADY_EXISTS The records whose key is the same as @a key
 * exists in masstree, so this function returned immediately.
 * @return Status::OK It inserted record.
 */
extern Status insert_record_to_masstree(char const* key,  // NOLINT
                                        std::size_t len_key, Record* record);

}  // namespace shirakami
