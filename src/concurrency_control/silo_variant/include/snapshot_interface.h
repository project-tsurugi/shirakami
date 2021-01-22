/**
 * @file snapshot_interface.h
 */

#include "kvs/scheme.h"

namespace shirakami::cc_silo_variant::snapshot_interface {

/**
 * @pre This func is called by search_key.
 * @param token
 * @param key
 * @param ret_tuple
 * @return
 */
extern Status lookup_snapshot(Token token, std::string_view key, Tuple** ret_tuple); // NOLINT

extern Status
scan_key(Token token, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end,
         std::vector<const Tuple*> &result);

} // namespace shirakami::cc_silo_variant::snapshot_interface