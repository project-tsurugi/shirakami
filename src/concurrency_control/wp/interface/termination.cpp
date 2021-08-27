
#include "shirakami/interface.h"

namespace shirakami {

Status abort([[maybe_unused]] Token token) { // NOLINT
    return Status::OK;
}

extern Status commit([[maybe_unused]] Token token, [[maybe_unused]] commit_param* cp) { // NOLINT
    return Status::OK;
}

extern bool check_commit([[maybe_unused]] Token token, [[maybe_unused]] std::uint64_t commit_id) { // NOLINT
    return true;
}

} // namespace shirakami
