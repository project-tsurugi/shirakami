#include "concurrency_control/wp/include/garbage.h"

namespace shirakami {

void garbage::gc_handle::destroy() {
    while (!val_cont_.empty()) {
        value_type val{};
        val_cont_.try_pop(val);
        if (val.first != nullptr) {
            delete val.first; // NOLINT
            val = {};
        }
    }
    val_cont_.clear();
}

} // namespace shirakami