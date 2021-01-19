/**
 * @file snapshot_interface.h
 */

#include "kvs/scheme.h"

namespace shirakami::cc_silo_variant::snapshot_interface {

extern Status lookup_snapshot(Token token, std::string_view key, Tuple** ret_tuple); // NOLINT

} // namespace shirakami::cc_silo_variant::snapshot_interface