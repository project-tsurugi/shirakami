/**
 * @file src/concurrency_control/include/local_set.h
 */

#pragma once

#include "local_set_scheme.h"

namespace shirakami {

class local_read_set {
public:
private:
    std::vector<read_set_obj> cont_for_short_;
};

class local_write_set {
public:
private:
    std::vector<write_set_obj> cont_for_short_;
};

} // namespace shirakami