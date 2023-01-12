
#include <string_view>

#include "concurrency_control/include/record.h"

#include "index/yakushima/include/scheme.h"

#include "shirakami/logging.h"
#include "shirakami/storage_options.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

template<class Record>
Status get(Storage st, std::string_view const key, Record*& rec_ptr) {
    std::pair<Record**, std::size_t> out{};
    auto rc{yakushima::get<Record*>({reinterpret_cast<char*>(&st), // NOLINT
                                     sizeof(st)},
                                    key, out)};
    if (rc == yakushima::status::OK) {
        rec_ptr = *out.first;
        return Status::OK;
    }
    if (rc == yakushima::status::WARN_NOT_EXIST) {
        return Status::WARN_NOT_FOUND;
    }
    if (rc == yakushima::status::WARN_STORAGE_NOT_EXIST) {
        return Status::WARN_STORAGE_NOT_FOUND;
    }
    LOG(ERROR) << log_location_prefix << "yakushima get error.";
    return Status::ERR_FATAL;
}

template<class Record>
yakushima::status put(yakushima::Token tk, Storage st, std::string_view key,
                      Record* rec_ptr, yakushima::node_version64*& nvp) {
    return yakushima::put<Record*>(
            tk, {reinterpret_cast<char*>(&st), sizeof(st)}, key,       // NOLINT
            &rec_ptr, sizeof(Record*), nullptr,                        // NOLINT
            static_cast<yakushima::value_align_type>(sizeof(Record*)), // NOLINT
            true, &nvp);
}

template<class Record>
yakushima::status put(yakushima::Token tk, Storage st, std::string_view key,
                      std::string_view val) {
    Record* rec_ptr = new Record(key, val); // NOLINT
    rec_ptr->reset_ts();
    yakushima::node_version64* nvp{};
    auto rc{put<Record>(tk, st, key, rec_ptr, nvp)};
    if (rc != yakushima::status::OK) { delete rec_ptr; } // NOLINT
    return rc;
}

template<class Record>
yakushima::status put(Storage st, std::string_view key, std::string_view val) {
    yakushima::Token tk{};
    auto rc{yakushima::enter(tk)};
    if (rc != yakushima::status::OK) { return rc; }
    rc = put<Record>(tk, st, key, val);
    yakushima::leave(tk);
    if (rc != yakushima::status::OK) { return rc; }
    return yakushima::status::OK;
}

/**
 * @brief Scan using yakushima.
 * @param st 
 * @param l_key 
 * @param l_end 
 * @param r_key 
 * @param r_end 
 * @param max_size 
 * @param scan_res 
 * @param nvec 
 * @return Status::OK It found some records.
 * @return Status::WARN_NOT_FOUND It found no records.
 * @return Status::WARN_STORAGE_NOT_FOUND It didn't find the @a st.
 */
[[maybe_unused]] static inline Status
scan(Storage st, std::string_view const l_key, scan_endpoint const l_end,
     std::string_view const r_key, scan_endpoint const r_end,
     std::size_t const max_size,
     std::vector<std::tuple<std::string, Record**, std::size_t>>& scan_res,
     std::vector<std::pair<yakushima::node_version64_body,
                           yakushima::node_version64*>>* nvec) {
    auto rc{yakushima::scan(
            {reinterpret_cast<char*>(&st), sizeof(st)}, // NOLINT
            l_key, parse_scan_endpoint(l_end), r_key,
            parse_scan_endpoint(r_end), scan_res, nvec, max_size)};
    if (rc == yakushima::status::WARN_STORAGE_NOT_EXIST) {
        return Status::WARN_STORAGE_NOT_FOUND;
    }
    if (rc == yakushima::status::WARN_NOT_EXIST ||
        rc == yakushima::status::OK_ROOT_IS_NULL) {
        return Status::WARN_NOT_FOUND;
    }
    if (rc == yakushima::status::OK) { return Status::OK; }
    LOG(ERROR) << log_location_prefix << "yakushima scan error " << rc;
    return Status::ERR_FATAL;
}

static inline Status remove(yakushima::Token tk, Storage st,
                            std::string_view key) {
    auto rc{yakushima::remove(
            tk, {reinterpret_cast<char*>(&st), sizeof(st)}, // NOLINT
            key)};
    if (yakushima::status::OK != rc) { return Status::INTERNAL_WARN_NOT_FOUND; }
    return Status::OK;
}

} // namespace shirakami