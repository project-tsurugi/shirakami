
#include "shirakami/interface.h"

namespace shirakami {

Status close_scan([[maybe_unused]] Token token,
                  [[maybe_unused]] ScanHandle handle) { // NOLINT
    return Status::OK;
}

Status open_scan([[maybe_unused]] Token token, [[maybe_unused]] Storage storage,
                 [[maybe_unused]] const std::string_view l_key, // NOLINT
                 [[maybe_unused]] const scan_endpoint l_end,
                 [[maybe_unused]] const std::string_view r_key,
                 [[maybe_unused]] const scan_endpoint r_end,
                 [[maybe_unused]] ScanHandle& handle,
                 [[maybe_unused]] std::size_t const max_size) {
    return Status::OK;
}

[[maybe_unused]] Status
scannable_total_index_size([[maybe_unused]] Token token, // NOLINT
                           [[maybe_unused]] ScanHandle handle,
                           [[maybe_unused]] std::size_t& size) {
    return Status::OK;
}

} // namespace shirakami
