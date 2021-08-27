
#include "shirakami/interface.h"

namespace shirakami {

Status insert([[maybe_unused]] Token token, [[maybe_unused]] Storage storage, [[maybe_unused]] const std::string_view key, // NOLINT
              [[maybe_unused]] const std::string_view val) {
    return Status::OK;
}

} // namespace shirakami
