/**
 * @file interface_search.cpp
 * @brief implement about transaction
 */

#include <bitset>

#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/interface.h"
#endif  // CC_SILO_VARIANT
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

Status search_key(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                  const char* key, std::size_t len_key,
                  Tuple** tuple) {
#ifdef CC_SILO_VARIANT
  return silo_variant::search_key(token, storage, key, len_key, tuple);
#endif
}

}  //  namespace shirakami
