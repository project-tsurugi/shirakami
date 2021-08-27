

#include "shirakami/interface.h"

namespace shirakami {

[[maybe_unused]] Status delete_all_records() { // NOLINT
    return Status::OK;
}

Status delete_record([[maybe_unused]] Token token, [[maybe_unused]] Storage storage, [[maybe_unused]] const std::string_view key) { // NOLINT
    return Status::OK;
}

} // namespace shirakami