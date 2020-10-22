/**
 * @file tid.concurrency_control
 * @details implement about tid.
 */

#include "concurrency_control/silo_variant/include/tid.h"

#include <bitset>
#include <iostream>

namespace shirakami::cc_silo_variant {

void tid_word::display() {
    std::cout << "obj_ : " << std::bitset<sizeof(obj_) * 8>(obj_)  // NOLINT
              << std::endl;                                        // NOLINT
    std::cout << "lock_ : " << lock_ << std::endl;                 // NOLINT
    std::cout << "latest_ : " << latest_ << std::endl;             // NOLINT
    std::cout << "absent_ : " << absent_ << std::endl;             // NOLINT
    std::cout << "tid_ : " << tid_ << std::endl;                   // NOLINT
    std::cout << "epoch_ : " << epoch_ << std::endl;               // NOLINT
}

}  // namespace shirakami::silo_variant
