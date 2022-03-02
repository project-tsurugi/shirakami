
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/wp/include/session.h"

#include "index/yakushima/include/scheme.h"

#include "shirakami/interface.h"

namespace shirakami {

Status close_scan(Token const token, ScanHandle const handle) {
    auto* ti = static_cast<session*>(token);

    return ti->get_scan_handle().clear(handle);
}

inline Status find_open_scan_slot(session* const ti, ScanHandle& out) {
    auto& sh = ti->get_scan_handle();
    for (ScanHandle i = 0;; ++i) {
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

Status open_scan(Token const token, Storage storage,
                 const std::string_view l_key, const scan_endpoint l_end,
                 const std::string_view r_key, const scan_endpoint r_end,
                 ScanHandle& handle, std::size_t const max_size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        // todo stale snapshot read only tx mode.
        return Status::ERR_NOT_IMPLEMENTED;
    }

    auto rc{find_open_scan_slot(ti, handle)};
    if (rc != Status::OK) { return rc; }

    // scan for index
    std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
    constexpr std::size_t index_rec_ptr{1};
    std::vector<std::pair<yakushima::node_version64_body,
                          yakushima::node_version64*>>
            nvec;
    constexpr std::size_t index_nvec_body{0};
    constexpr std::size_t index_nvec_ptr{1};
    yakushima::scan(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            l_key, parse_scan_endpoint(l_end), r_key,
            parse_scan_endpoint(r_end), scan_res, &nvec, max_size);
    if (scan_res.empty()) {
        /**
         * scan couldn't find any records.
         */
        return Status::WARN_NOT_FOUND;
    }

    /**
     * You must ensure that new elements are not interrupted in the range at 
     * the node that did not retrieve the element but scanned it when masstree 
     * scanned it.
     */
    std::size_t nvec_delta{0};
    if (scan_res.size() < nvec.size()) {
        auto add_ns = [&ti, &nvec](std::size_t n) {
            for (std::size_t i = 0; i < n; ++i) {
                ti->get_node_set().emplace_back(nvec.at(i));
            }
        };
        if (scan_res.size() + 1 == nvec.size()) {
            nvec_delta = 1;
            add_ns(1);

        } else if (scan_res.size() + 2 == nvec.size()) {
            nvec_delta = 2;
            add_ns(2);
        }
    }

    // Cache a pointer to record.
    auto& sh = ti->get_scan_handle();
    std::get<scan_handler::scan_cache_storage_pos>(
            sh.get_scan_cache()[handle]) = storage;
    auto& vec = std::get<scan_handler::scan_cache_vec_pos>(
            sh.get_scan_cache()[handle]);
    vec.reserve(scan_res.size());
    for (std::size_t i = 0; i < scan_res.size(); ++i) {
        vec.emplace_back(*std::get<index_rec_ptr>(scan_res.at(i)),
                         std::get<index_nvec_body>(nvec.at(i + nvec_delta)),
                         std::get<index_nvec_ptr>(nvec.at(i + nvec_delta)));
    }

    return Status::OK;
}

Status next(Token const token, ScanHandle const handle) {
    auto* ti = static_cast<session*>(token);

    auto& sh = ti->get_scan_handle();
    // increment cursor
    std::size_t& scan_index = sh.get_scan_cache_itr()[handle];
    ++scan_index;

    // check range of cursor
    if (std::get<scan_handler::scan_cache_vec_pos>(sh.get_scan_cache()[handle])
                .size() <= scan_index) {
        return Status::WARN_SCAN_LIMIT;
    }

    // reset cache in cursor
    ti->get_scan_handle().get_ci(handle).reset();
    return Status::OK;
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

[[maybe_unused]] Status scannable_total_index_size(Token const token,
                                                   ScanHandle const handle,
                                                   std::size_t& size) {
    auto* ti = static_cast<session*>(token);
    auto& sh = ti->get_scan_handle();

    if (sh.get_scan_cache().find(handle) == sh.get_scan_cache().end()) {
        /**
         * the handle was invalid.
         */
        return Status::WARN_INVALID_HANDLE;
    }

    size = std::get<scan_handler::scan_cache_vec_pos>(
                   sh.get_scan_cache()[handle])
                   .size();
    return Status::OK;
}

} // namespace shirakami
