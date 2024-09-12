/**
 * @file
 */

#pragma once

#include <cstdlib>
#include <ostream>
#include <string_view>

namespace shirakami {

enum class OP_TYPE : std::int32_t {
    ABORT,
    BEGIN,
    COMMIT,
    DELETE,
    DELSERT,
    INSERT,
    NONE,
    SCAN,
    SEARCH,
    TOMBSTONE,
    UPDATE,
    UPSERT,
};

inline constexpr std::string_view to_string_view(const OP_TYPE op) noexcept {
    using namespace std::string_view_literals;
    switch (op) {
        case OP_TYPE::ABORT:
            return "ABORT"sv;
        case OP_TYPE::BEGIN:
            return "BEGIN"sv;
        case OP_TYPE::COMMIT:
            return "COMMIT"sv;
        case OP_TYPE::DELETE:
            return "DELETE"sv;
        case OP_TYPE::DELSERT:
            return "DELSERT"sv;
        case OP_TYPE::INSERT:
            return "INSERT"sv;
        case OP_TYPE::NONE:
            return "NONE"sv;
        case OP_TYPE::SCAN:
            return "SCAN"sv;
        case OP_TYPE::SEARCH:
            return "SEARCH"sv;
        case OP_TYPE::TOMBSTONE:
            return "TOMBSTONE"sv;
        case OP_TYPE::UPDATE:
            return "UPDATE"sv;
        case OP_TYPE::UPSERT:
            return "UPSERT"sv;
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, const OP_TYPE op) {
    return out << to_string_view(op);
}

} // namespace shirakami
