#pragma once

#include "shirakami/interface.h"

namespace shirakami {

inline log_event_callback log_event_callback_; // NOLINT

[[maybe_unused]] static void clear_log_event_callback() {
    log_event_callback f;
    log_event_callback_ = f;
}

[[maybe_unused]] static log_event_callback get_log_event_callback() {
    return log_event_callback_;
}

[[maybe_unused]] static void
set_log_event_callback(log_event_callback const& callback) {
    log_event_callback_ = callback;
}

} // namespace shirakami