//
// Created by thawk on 2021/01/19.
//

#include "kvs/tuple.h"

#include "concurrency_control/silo_variant/include/snapshot_interface.h"

namespace shirakami::cc_silo_variant::snapshot_interface {

Status lookup_snapshot([[maybe_unused]]Token token, [[maybe_unused]]std::string_view key, [[maybe_unused]]Tuple** const ret_tuple) {
    return Status::OK;
}

}
