#pragma once

#include <string_view>
#include <vector>

#include "scheme.h"
#include "storage_options.h"

namespace shirakami {

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
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length
 * should be equal or less than 30KB.
 * @return Status::WARN_STORAGE_ID_DEPLETION You may use @a options.id_ larger
 * than 2^32, or shirakami create storage more than 2^32.
 * Please review your usage.
 * @deprecated this function is deprecated and left only for testing and compatibility.
 * Use create_storage() function below that does not receive storage key as parameter.
 */
Status create_storage(std::string_view key, Storage& storage,
                      storage_option const& options = {}); // NOLINT

/**
 * @brief Create new storage, and return its handler.
 * New storage key is automatically generated and assigned to the created storage.
 * The generated storage key can be retrieved by get_storage_key().
 * @param[out] storage The storage handle mapped for the generated key.
 * @param[in] options If you don't use this argument, @a storage is specified
 * by shirakami, otherwise, is specified by user.
 * @return Status::OK if successful.
 * @return Status::WARN_ALREADY_EXISTS You may use @a options.id_ more than once.
 * @return Status::WARN_STORAGE_ID_DEPLETION You may use @a options.id_ larger
 * than 2^32, or shirakami create storage more than 2^32.
 * Please review your usage.
 */
Status create_storage(Storage& storage, storage_option const& options = {});

/**
 * @brief delete existing storage and records under the storage.
 * @param[in] storage the storage handle retrieved with create_storage().
 * @return Status::OK if successful.
 * @return Status::WARN_INVALID_HANDLE if the storage is not registered with
 * the given name.
 * @return Status::ERR_FATAL Some programming error.
 */
Status delete_storage(Storage storage);

/**
 * @brief Get the storage handle by using key.
 * @param[in] key The key of the target storage handle.
 * @param[out] out The target storage handle.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length
 * should be equal or less than 30KB.
 * @return Status::WARN_NOT_FOUND not found.
 */
Status get_storage(std::string_view key, Storage& out);

/**
 * @brief Get a list of existing storage key.
 * @param[out] out the list of existing storage.
 * @return Status::OK success including out is empty.
 */
Status list_storage(std::vector<std::string>& out);

/**
 * @brief Get storage options.
 * @param[in] storage the storage handle.
 * @param[out] options The target storage options.
 * @return Status::OK success.
 * @return Status::WARN_ILLEGAL_OPERATION There are many conflict
 * about @a storage between this function and @a storage_set_options.
 * @return Status::WARN_NOT_FOUND The storage was not found.
 * @return Status::ERR_FATAL Error about invalid use. It couldn't find storage
 * meta information internally.
 */
Status storage_get_options(Storage storage, storage_option& options);

/**
 * @brief Set storage options.
 * @param[in] storage the storage handle.
 * @param[in] options The source of setting.
 * @return Status::OK success.
 * @return Status::WARN_NOT_FOUND The storage was not found.
 * @return Status::ERR_FATAL Error about invalid use. It couldn't find storage
 * meta information internally.
 */
Status storage_set_options(Storage storage, storage_option const& options);

/**
 * @brief get the storage key for the given storage handle.
 * @param[in] storage The storage handle.
 * @param[out] key The key associated with the storage handle.
 * @return Status::OK success.
 * @return Status::WARN_NOT_FOUND The storage was not found.
 */
Status get_storage_key(Storage storage, std::string& key);

} // namespace shirakami
