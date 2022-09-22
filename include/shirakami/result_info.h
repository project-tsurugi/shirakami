#pragma once

#include <string_view>

namespace shirakami {

/**
 * @brief Reason code. This is a secondary information about failed result after
 * commit command.
 */
enum class reason_code : std::int32_t {
    /**
     * @brief undefined code.
     */
    UNKNOWN,
    /**
     * @brief Protecting committed transactional read operation.
     */
    COMMITTED_READ_PROTECTION,
    /**
     * @brief Deleting non-existing record.
     */
    DELETE_NON_EXISTING_RECORD,
    /**
     * @brief Inserting a key which is same to existing one.
     */
    INSERT_EXISTING_KEY,
    /**
     * @brief The low priority ltx found high priority ltx's write preserve, 
     * and tried forwarding but the forwarding break old own read.
     */
    FORWARDING_BLOCKED_BY_READ,
    /**
     * @brief Occ tx detected write preserve.
     */
    OCC_DETECT_WRITE_PRESERVE,
    /**
     * @brief Occ tx failed read validation.
     */
    OCC_READ_VALIDATION,
    /**
     * @brief Abort due to phantom avoidance function.
     */
    PHANTOM_AVOIDANCE_DETECTED,
    /**
     * @brief Updating non-existing record.
     */
    UPDATE_NON_EXISTING_RECORD,
    /**
     * @brief After abort command by user.
     */
    USER_ABORT,
    /**
     * @brief violation of read area at tx_begin.
     */
    VIOLATE_READ_AREA,
};

inline constexpr std::string_view to_string_view(reason_code rc) noexcept {
    using namespace std::string_view_literals;
    switch (rc) {
        case reason_code::UNKNOWN:
            return "UNKNOWN"sv; // NOLINT
        case reason_code::COMMITTED_READ_PROTECTION:
            return "COMMITTED_READ_PROTECTION"sv; // NOLINT
        case reason_code::DELETE_NON_EXISTING_RECORD:
            return "DELETE_NON_EXISTING_RECORD"sv; // NOLINT
        case reason_code::INSERT_EXISTING_KEY:
            return "INSERT_EXISTING_KEY"sv; // NOLINT
        case reason_code::FORWARDING_BLOCKED_BY_READ:
            return "FORWARDING_BLOCKED_BY_READ"sv; // NOLINT
        case reason_code::OCC_DETECT_WRITE_PRESERVE:
            return "OCC_DETECT_WRITE_PRESERVE"sv; // NOLINT
        case reason_code::OCC_READ_VALIDATION:
            return "OCC_READ_VALIDATION"sv; // NOLINT
        case reason_code::PHANTOM_AVOIDANCE_DETECTED:
            return "PHANTOM_AVOIDANCE_DETECTED"sv; //NOLINT
        case reason_code::UPDATE_NON_EXISTING_RECORD:
            return "UPDATE_NON_EXISTING_RECORD"sv; // NOLINT
        case reason_code::USER_ABORT:
            return "USER_ABORT"sv; // NOLINT
        case reason_code::VIOLATE_READ_AREA:
            return "VIOLATE_READ_AREA"sv; // NOLINT
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