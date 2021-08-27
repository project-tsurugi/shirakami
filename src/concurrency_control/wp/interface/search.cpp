
#include "shirakami/interface.h"

namespace shirakami {

Status search_key([[maybe_unused]] Token token, [[maybe_unused]] Storage storage, [[maybe_unused]] const std::string_view key, // NOLINT
                  [[maybe_unused]] Tuple** const tuple) {
    return Status::OK;
}

} // namespace shirakami
