/**
 * @file interface_helper.cpp
 * @brief implement about transaction
 */

#include <bitset>

#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/interface.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

Status enter(Token& token) {  // NOLINT
#ifdef INDEX_KOHLER_MASSTREE
  masstree_wrapper<silo_variant::Record>::thread_init(sched_getcpu());
#endif

#ifdef CC_SILO_VARIANT
  return silo_variant::enter(token);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

void fin() {
#ifdef CC_SILO_VARIANT
  return silo_variant::fin();
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status init(std::string_view log_directory_path) {  // NOLINT
#ifdef CC_SILO_VARIANT
  return silo_variant::init(log_directory_path);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status leave(Token token) {  // NOLINT
#ifdef CC_SILO_VARIANT
  return silo_variant::leave(token);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

}  //  namespace shirakami
