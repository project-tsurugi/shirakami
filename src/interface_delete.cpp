/**
 * @file interface_delete.cpp
 * @brief implement about transaction
 */

#include <bitset>

#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/interface.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // NOLINT
// sizeof(Tuple)

namespace shirakami {

[[maybe_unused]] Status delete_all_records() {  // NOLINT
#ifdef CC_SILO_VARIANT
  return silo_variant::delete_all_records();
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status delete_record(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                     const char* key, std::size_t len_key) {
#ifdef CC_SILO_VARIANT
  return silo_variant::delete_record(token, storage, key, len_key);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

}  //  namespace shirakami
