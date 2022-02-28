
#include <string_view>

#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

namespace shirakami {

template<class Record>
Status get(Storage st, std::string_view const key, Record*& rec_ptr) {
    Record** rec_d_ptr{
            yakushima::get<Record*>({reinterpret_cast<char*>(&st), // NOLINT
                                     sizeof(st)},
                                    key)
                    .first};
    if (rec_d_ptr == nullptr) { return Status::WARN_NOT_FOUND; }
    rec_ptr = *rec_d_ptr;
    return Status::OK;
}

template<class Record>
yakushima::status put(yakushima::Token tk, Storage st, std::string_view key,
                      Record* rec_ptr, yakushima::node_version64*& nvp) {
    return yakushima::put<Record*>(
            tk, {reinterpret_cast<char*>(&st), sizeof(st)}, key,       // NOLINT
            &rec_ptr, sizeof(Record*), nullptr,                        // NOLINT
            static_cast<yakushima::value_align_type>(sizeof(Record*)), // NOLINT
            &nvp);
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

} // namespace shirakami