#pragma once

#include "shirakami/interface.h"

namespace shirakami {

inline log_event_callback log_event_callback_;

void clear_log_event_callback() {
    log_event_callback f;
    log_event_callback_ = f;
}

log_event_callback get_log_event_callback() { return log_event_callback_; }

void set_log_event_callback(log_event_callback callback) {
    log_event_callback_ = callback;
}

} // namespace shirakami