/**
 * @file interface_scan.cpp
 * @detail implement about scan operation.
 */

#include <map>

#include "cc/silo_variant/include/interface.h"
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

Status close_scan(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                  const ScanHandle handle) {
#ifdef CC_SILO_VARIANT
  return silo_variant::close_scan(token, storage, handle);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status open_scan(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                 const char* lkey, std::size_t len_lkey, bool l_exclusive,
                 const char* rkey, std::size_t len_rkey, bool r_exclusive,
                 ScanHandle& handle) {
#ifdef CC_SILO_VARIANT
  return silo_variant::open_scan(token, storage, lkey, len_lkey, l_exclusive,
                                 rkey, len_rkey, r_exclusive, handle);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status read_from_scan(Token token,  // NOLINT
                      [[maybe_unused]] Storage storage, const ScanHandle handle,
                      Tuple** const tuple) {
#ifdef CC_SILO_VARIANT
  return silo_variant::read_from_scan(token, storage, handle, tuple);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status scan_key(Token token, [[maybe_unused]] Storage storage,  // NOLINT
                const char* lkey, std::size_t len_lkey, bool l_exclusive,
                const char* rkey, std::size_t len_rkey, bool r_exclusive,
                std::vector<const Tuple*>& result) {
#ifdef CC_SILO_VARIANT
  return silo_variant::scan_key(token, storage, lkey, len_lkey, l_exclusive,
                                rkey, len_rkey, r_exclusive, result);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

Status scannable_total_index_size(Token token,  // NOLINT
                                  [[maybe_unused]] Storage storage,
                                  ScanHandle& handle, std::size_t& size) {
#ifdef CC_SILO_VARIANT
  return silo_variant::scannable_total_index_size(token, storage, handle, size);
#else
  std::cout << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
  std::abort();
#endif
}

}  // namespace shirakami
