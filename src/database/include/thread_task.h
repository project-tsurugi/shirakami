#pragma once

#include <atomic>

#include "shirakami/scheme.h"

// third_party/
#include "glog/logging.h"

namespace shirakami {

enum class task_kind : std::int32_t {
    UNKNOWN,
    LTX_COMMIT_WHOLE,
};

class thread_task {
public:
    // getter
    [[nodiscard]] task_kind get_task_kind() const { return task_kind_; }

    [[nodiscard]] Token get_token() const { return token_; }

    [[nodiscard]] bool get_completed() const {
        return completed_.load(std::memory_order_acquire);
    }

    // setter
    void set_task_kind(task_kind tk) { task_kind_ = tk; }

    void set_token(Token token) { token_ = token; }

    void set_completed(bool tf) {
        completed_.store(tf, std::memory_order_release);
    }

private:
    task_kind task_kind_{task_kind::UNKNOWN};

    Token token_{};

    std::atomic<bool> completed_{false};
};

} // namespace shirakami
