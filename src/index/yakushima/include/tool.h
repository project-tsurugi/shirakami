/**
 * @file src/index/yakushima/include/tool.h
 */

#pragma once

#include "yakushima/include/version.h"

namespace shirakami {

/**
 * @brief compare yakushima version about vinsert_delete and vsplit.
 * @return true same
 * @return false different
*/
static inline bool
comp_ver_for_node_verify(yakushima::node_version64_body const left,
                         yakushima::node_version64_body const right) {
    // compare vinsert_delete
    if (left.get_vinsert_delete() != right.get_vinsert_delete()) {
        return false;
    }
    // compare vsplit
    if (left.get_vsplit() != right.get_vsplit()) { return false; }
    // same
    return true;
}

} // namespace shirakami