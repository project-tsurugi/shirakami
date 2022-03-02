
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/wp/include/session.h"

#include "shirakami/interface.h"

namespace shirakami {

Status close_scan(Token token, ScanHandle handle) {
    auto* ti = static_cast<session*>(token);

    return ti->get_scan_handle().clear(handle);
}

inline Status find_open_scan_slot(session* ti, ScanHandle& out) {
    auto& sh = ti->get_scan_handle();
    for (ScanHandle i = 0; ; ++i) {
        auto itr = sh.get_scan_cache().find(i);
        if (itr == sh.get_scan_cache().end()) {
            out = i;
            // clear cursor info
            sh.get_scan_cache_itr()[i] = 0;
            return Status::OK;
        }
    }
    return Status::WARN_SCAN_LIMIT;
}

Status open_scan(Token token, [[maybe_unused]] Storage storage,
                 [[maybe_unused]] const std::string_view l_key,
                 [[maybe_unused]] const scan_endpoint l_end,
                 [[maybe_unused]] const std::string_view r_key,
                 [[maybe_unused]] const scan_endpoint r_end,
                 [[maybe_unused]] ScanHandle& handle,
                 [[maybe_unused]] std::size_t const max_size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        // todo stale snapshot read only tx mode.
        return Status::ERR_NOT_IMPLEMENTED;
    }

    auto rc{find_open_scan_slot(ti, handle)};
    if (rc != Status::OK) { return rc; }

    return Status::ERR_NOT_IMPLEMENTED;
}

Status next([[maybe_unused]] Token token, [[maybe_unused]] ScanHandle handle) {
    return Status::ERR_NOT_IMPLEMENTED;
}

Status read_key_from_scan([[maybe_unused]] Token token,
                          [[maybe_unused]] ScanHandle handle,
                          [[maybe_unused]] std::string& key) {
    return Status::ERR_NOT_IMPLEMENTED;
}

Status read_value_from_scan([[maybe_unused]] Token token,
                            [[maybe_unused]] ScanHandle handle,
                            [[maybe_unused]] std::string& value) {
    return Status::ERR_NOT_IMPLEMENTED;
}

[[maybe_unused]] Status
scannable_total_index_size([[maybe_unused]] Token token,
                           [[maybe_unused]] ScanHandle handle,
                           [[maybe_unused]] std::size_t& size) {
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami
