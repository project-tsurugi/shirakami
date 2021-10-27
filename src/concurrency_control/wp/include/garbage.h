#pragma once

#include "concurrent_queue.h"
#include "epoch.h"

namespace shirakami::garbage {

class gc_handle {
public:
    using value_type = std::pair<std::string*, epoch::epoch_t>;

    void destroy();

    void push_value(value_type g_val) {
        val_cont_.push(g_val);
    }

private:
    concurrent_queue<value_type> val_cont_;
};

} // namespace shirakami::garbage