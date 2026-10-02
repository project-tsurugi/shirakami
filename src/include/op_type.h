/**
 * @file
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <iostream>

namespace shirakami {

struct OP_TYPE {
    using base_int_type = std::uint32_t;
    static constexpr base_int_type WSO_BASE = 0x100U;
    static constexpr base_int_type WSO_FROM_ABSENT = (WSO_BASE | 0U);
    static constexpr base_int_type WSO_FROM_ALIVE  = (WSO_BASE | 1U);
    static constexpr base_int_type WSO_FROM_ANY    = (WSO_BASE | 2U);
    static constexpr base_int_type WSO_FROM_MASK   = WSO_FROM_ABSENT | WSO_FROM_ALIVE | WSO_FROM_ANY;
    static constexpr base_int_type WSO_TO_ABSENT   = (WSO_BASE | (0U << 2U));
    static constexpr base_int_type WSO_TO_ALIVE    = (WSO_BASE | (1U << 2U));
    static constexpr base_int_type WSO_TO_MASK     = WSO_TO_ABSENT | WSO_TO_ALIVE;

    enum OP_TYPE_E : base_int_type { // NOLINT
    ABORT,
    BEGIN,
    COMMIT,
    NONE,
    SCAN,
    SEARCH,
    // WSO OP
    DELETE    = WSO_FROM_ALIVE  | WSO_TO_ABSENT,
    DELSERT   = WSO_FROM_ANY    | WSO_TO_ABSENT,
    INSERT    = WSO_FROM_ABSENT | WSO_TO_ALIVE,
    TOMBSTONE = WSO_FROM_ABSENT | WSO_TO_ABSENT,
    UPDATE    = WSO_FROM_ALIVE  | WSO_TO_ALIVE,
    UPSERT    = WSO_FROM_ANY    | WSO_TO_ALIVE,
    };
static_assert(SEARCH < WSO_BASE);

private:
    OP_TYPE_E op;
public:
    OP_TYPE() = default;
    constexpr OP_TYPE(enum OP_TYPE_E type) : op(type) {} // NOLINT
    // constexpr bool operator==(OP_TYPE a) const noexcept { return a.op == op; }
    // constexpr bool operator!=(OP_TYPE a) const noexcept { return a.op != op; }
    constexpr operator OP_TYPE_E() const { return op; } // NOLINT
    explicit operator bool() const = delete;

[[nodiscard]] inline constexpr bool is_wso_from_absent() const noexcept {
    return (op & WSO_FROM_MASK) == WSO_FROM_ABSENT;
}

[[nodiscard]] inline constexpr bool is_wso_from_alive() const noexcept {
    return (op & WSO_FROM_MASK) == WSO_FROM_ALIVE;
}

[[nodiscard]] inline constexpr bool is_wso_from_any() const noexcept {
    return (op & WSO_FROM_MASK) == WSO_FROM_ANY;
}

[[nodiscard]] inline constexpr bool is_wso_to_absent() const noexcept {
    return (op & WSO_TO_MASK) == WSO_TO_ABSENT;
}

[[nodiscard]] inline constexpr bool is_wso_to_alive() const noexcept {
    return (op & WSO_TO_MASK) == WSO_TO_ALIVE;
}

[[nodiscard]] inline constexpr OP_TYPE of_wso_to_absent() const noexcept {
    return OP_TYPE{static_cast<OP_TYPE_E>((op & ~WSO_TO_MASK) | WSO_TO_ABSENT)};
}

[[nodiscard]] inline constexpr OP_TYPE of_wso_to_alive() const noexcept {
    return OP_TYPE{static_cast<OP_TYPE_E>((op & ~WSO_TO_MASK) | WSO_TO_ALIVE)};
}

[[nodiscard]] inline constexpr std::string_view to_string_view() const noexcept {
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
};

inline std::ostream& operator<<(std::ostream& out, const OP_TYPE op) {
    return out << op.to_string_view();
}

} // namespace shirakami
