/**
 * @file src/concurrency_control/include/local_set.h
 */

#pragma once

#include <map>
#include <utility>

#include "local_set_scheme.h"

namespace shirakami {

class local_read_set {
public:
private:
    /**
     * @brief A flag that identifies whether the container is for batch processing or online processing.
     */
    [[maybe_unused]] bool for_batch_{false};
    [[maybe_unused]] std::vector<read_set_obj> cont_for_ol_;
    [[maybe_unused]] std::map<Record*, read_set_obj> cont_for_bt_;
};

class local_write_set {
public:
private:
    /**
     * @brief A flag that identifies whether the container is for batch processing or online processing.
     */
    [[maybe_unused]] bool for_batch_{false};
    [[maybe_unused]] std::vector<write_set_obj> cont_for_ol_;
    [[maybe_unused]] std::map<Record*, write_set_obj> cont_for_bt_;
};

} // namespace shirakami