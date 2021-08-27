
#include "shirakami/interface.h"

namespace shirakami {

Status enter([[maybe_unused]] Token& token) { // NOLINT
    return Status::OK;
}

void fin([[maybe_unused]] bool force_shut_down_cpr) try {
} catch (std::exception& e) {
    std::cerr << "fin() : " << e.what() << std::endl;
}

Status init([[maybe_unused]] bool enable_recovery, [[maybe_unused]] const std::string_view log_directory_path) { // NOLINT
    return Status::OK;
}

Status leave([[maybe_unused]] Token const token) { // NOLINT
    return Status::OK;
}

void tx_begin([[maybe_unused]] Token const token, [[maybe_unused]] bool const read_only, [[maybe_unused]] bool const for_batch) { // NOLINT
}

} // namespace shirakami