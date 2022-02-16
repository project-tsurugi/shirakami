
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
            tk, {reinterpret_cast<char*>(&st), sizeof(st)}, key,
            &rec_ptr,                                                  // NOLINT
            sizeof(Record*), nullptr,                                  // NOLINT
            static_cast<yakushima::value_align_type>(sizeof(Record*)), // NOLINT
            &nvp);
}

} // namespace shirakami