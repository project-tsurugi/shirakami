/**
 * @file interface_termination.cpp
 * @brief implement about transaction
 */

#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/interface.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // NOLINT
// sizeof(Tuple)

namespace shirakami {

Status abort(Token token) {  // NOLINT
#ifdef CC_SILO_VARIANT
  return silo_variant::abort(token);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status commit(Token token) {  // NOLINT
#ifdef CC_SILO_VARIANT
  return silo_variant::commit(token);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

}  //  namespace shirakami
