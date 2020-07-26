/**
 * @file interface_update_insert.cpp
 * @brief implement about transaction
 */

#include <bitset>

#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/interface.h"
#endif  // CC_SILO_VARIANT
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

Status insert(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              const char* const key, const std::size_t len_key,
              const char* const val, const std::size_t len_val) {
#ifdef CC_SILO_VARIANT
    return silo_variant::insert(token, storage, key, len_key, val, len_val);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status update(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              const char* const key, const std::size_t len_key,
              const char* const val, const std::size_t len_val) {
#ifdef CC_SILO_VARIANT
    return silo_variant::update(token, storage, key, len_key, val, len_val);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status upsert(Token token, [[maybe_unused]] Storage storage,  // NOLINT
              const char* const key, std::size_t len_key, const char* const val,
              std::size_t len_val) {
#ifdef CC_SILO_VARIANT
  return silo_variant::upsert(token, storage, key, len_key, val, len_val);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

}  //  namespace shirakami
