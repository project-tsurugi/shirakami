#pragma once

#include <string_view>

namespace shirakami {

/**
 * @brief Reason code. This is a secondary information about failed result.
 */
enum class reason_code : std::int32_t {
    /**
     * @brief committed. If you get the information against committed 
     * transaction, you may get this code.
     */
    COMMITTED,
    /**
     * @brief committed read protection.
     */
    COMMITTED_READ_PROTECTION,
    /**
     * @brief transactional delete operation.
     */
    DELETE,
    /**
     * @brief deadlock avoidance.
     */
    DEADLOCK_AVOIDANCE,
    /**
     * @brief transactional insert operation.
     */
    INSERT,
    /**
     * @brief read area option at tx_begin.
     */
    READ_AREA,
    /**
     * @brief read information.
     */
    READ_BY,
    /**
     * @brief read upper bound.
     */
    READ_UPPER_BOUND,
    /**
     * @brief read validation.
     */
    READ_VALIDATION,
    /**
     * @brief phantom avoidance.
     */
    PHANTOM_AVOIDANCE,
    /**
     * @brief transactional update operation.
     */
    UPDATE,
    /**
     * @brief write preserve.
     */
    WRITE_PRESERVE,
};

inline constexpr std::string_view to_string_view(reason_code rc) noexcept {
    using namespace std::string_view_literals;
    switch (rc) {
        case reason_code::COMMITTED:
            return "COMMITTED"sv; // NOLINT
        case reason_code::COMMITTED_READ_PROTECTION:
            return "COMMITTED_READ_PROTECTION"sv; // NOLINT
        case reason_code::DELETE:
            return "DELETE"sv; // NOLINT
        case reason_code::DEADLOCK_AVOIDANCE:
            return "DEADLOCK_AVOIDANCE"sv; // NOLINT
        case reason_code::INSERT:
            return "INSERT"sv; // NOLINT
        case reason_code::READ_AREA:
            return "READ_AREA"sv; // NOLINT
        case reason_code::READ_BY:
            return "READ_BY"sv; // NOLINT
        case reason_code::READ_UPPER_BOUND:
            return "READ_UPPER_BOUND"sv; // NOLINT
        case reason_code::READ_VALIDATION:
            return "READ_VALIDATION"sv; // NOLINT
        case reason_code::PHANTOM_AVOIDANCE:
            return "PHANTOM_AVOIDANCE"sv; // NOLINT
        case reason_code::UPDATE:
            return "UPDATE"sv; // NOLINT
        case reason_code::WRITE_PRESERVE:
            return "WRITE_PRESERVE"sv; // NOLINT
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, reason_code rc) { // NOLINT
    return out << to_string_view(rc);
}

class result_info {
public:
    result_info() = default;

    result_info(reason_code rc, std::string_view add_info)
        : reason_code_(rc), additional_information_(add_info) {}

    void clear_additional_information() { additional_information_.clear(); }

    reason_code get_reason_code() { return reason_code_; }

    std::string_view get_additional_information() {
        return additional_information_;
    }

    void set_reason_code(reason_code rc) { reason_code_ = rc; }

    void set_additional_information(std::string_view add_info) {
        additional_information_ = add_info;
    }

private:
    reason_code reason_code_{};
    std::string additional_information_{};
};

} // namespace shirakami