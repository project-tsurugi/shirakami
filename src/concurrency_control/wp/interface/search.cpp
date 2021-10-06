
#include "shirakami/interface.h"

namespace shirakami {

Status search_key([[maybe_unused]] Token token, [[maybe_unused]] Storage storage, [[maybe_unused]] const std::string_view key, // NOLINT
                  [[maybe_unused]] Tuple** const tuple) {
    // index access
    // version selection

    // occ
    // wp check
    // read version

    // batch
    // read version

    return Status::OK;
}

} // namespace shirakami
