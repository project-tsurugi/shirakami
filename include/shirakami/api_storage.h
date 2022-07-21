#pragma once

#include <string_view>
#include <vector>

#include "scheme.h"
#include "storage_options.h"

namespace shirakami {

/**
 * @brief Create one table by not using key, and return its handler.
 * @param[out] storage output parameter to pass the storage handle, that is 
 * used for the subsequent calls related with the storage.
 * Multiple create_storage calls assign storage value monotonically.
 * That is, Storage value assigned by create_storage is larger than the one 
 * assigned by previous call.
 * @param[in] options If you don't use this argument, @a storage is specified
 * by shirakami, otherwise, is specified by user.
 * @return Status::ERR_FATAL_INDEX Some programming error.
 * @return Status::OK if successful.
 * @return Status::WARN_ALREADY_EXISTS You may use @a options.id_ more than once.
 * @return Status::WARN_STORAGE_ID_DEPLETION You may use @a options.id_ larger 
 * than 2^32, or shirakami create storage more than 2^32.
 */
extern Status create_storage(Storage& storage,
                             storage_option options = {}); // NOLINT

/**
 * @brief Create one table by using key, and return its handler.
 * @param key The storage's key. It also can be used for get_storage.
 * @param storage The storage handle mapped for @a key.
 * @param[in] options If you don't use this argument, @a storage is specified
 * by shirakami, otherwise, is specified by user.
 * @return Status::ERR_FATAL_INDEX Some programming error.
 * @return Status::OK if successful.
 * @return Status::WARN_ALREADY_EXISTS You may use @a options.id_ or @a key 
 * more than once.
 * @return Status::WARN_STORAGE_ID_DEPLETION You may use @a options.id_ larger 
 * than 2^32, or shirakami create storage more than 2^32.
 */
extern Status create_storage(std::string_view key, Storage& storage,
                             storage_option options = {}); // NOLINT

/**
 * @brief Confirm existence of the storage.
 * @param[in] storage input parameter to confirm existence of the storage.
 * @return Status::OK if existence.
 * @return Status::WARN_NOT_FOUND if not existence.
 */
extern Status exist_storage(Storage storage);

/**
 * @brief delete existing storage and records under the storage.
 * @param[in] storage the storage handle retrieved with create_storage().
 * @return Status::OK if successful.
 * @return Status::WARN_INVALID_HANDLE if the storage is not registered with 
 * the given name.
 * @return Status::ERR_FATAL Some programming error.
 */
extern Status delete_storage(Storage storage);

/**
 * @brief Get the storage storage handle by using key.
 * @param[in] key The key of the target storage handle.
 * @param[out] out The target storage handle. 
 * @return Status::OK success.
 * @return Status::WARN_NOT_FOUND not found.
 */
extern Status get_storage(std::string_view key, Storage& out);

/**
 * @brief Get a list of existing storage.
 * @param[out] out the list of existing storage.
 * @return Status::OK if successful.
 * @return Status::WARN_NOT_FOUND if no storage.
 * @return Status::ERR_FATAL A serious error has been detected.
 */
extern Status list_storage(std::vector<Storage>& out);

} // namespace shirakami