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
     * @brief Deleting non-existing record.
     */
    KVS_DELETE,
    /**
     * @brief Inserting a key which is same to existing one.
     */
    KVS_INSERT,
    /**
     * @brief Updating non-existing record.
     */
    KVS_UPDATE,
    /**
     * @brief violation of read area at tx_begin.
     */
    CC_LTX_READ_AREA_VIOLATION,
    /**
     * @brief The low priority ltx found high priority ltx's write preserve, 
     * and tried forwarding but the forwarding break old own read.
     */
    CC_LTX_READ_UPPER_BOUND_VIOLATION,
    /**
     * @brief Protecting committed transactional read operation.
     */
    CC_LTX_WRITE_COMMITTED_READ_PROTECTION,
    /**
     * @brief Occ tx detected write preserve.
     */
    CC_OCC_WP_VERIFY,
    /**
     * @brief Occ tx failed read validation.
     */
    CC_OCC_READ_VERIFY,
    /**
     * @brief Abort due to phantom avoidance function.
     */
    CC_PHANTOM_AVOIDANCE,
    /**
     * @brief After abort command by user.
     */
    USER_ABORT,
};

inline constexpr std::string_view to_string_view(reason_code rc) noexcept {
    using namespace std::string_view_literals;
    switch (rc) {
        case reason_code::UNKNOWN:
            return "UNKNOWN"sv; // NOLINT
        case reason_code::KVS_DELETE:
            return "KVS_DELETE"sv; // NOLINT
        case reason_code::KVS_INSERT:
            return "KVS_INSERT"sv; // NOLINT
        case reason_code::KVS_UPDATE:
            return "KVS_UPDATE"sv; // NOLINT
        case reason_code::CC_LTX_READ_AREA_VIOLATION:
            return "CC_LTX_READ_AREA_VIOLATION"sv; // NOLINT
        case reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION:
            return "CC_LTX_READ_UPPER_BOUND_VIOLATION"sv; // NOLINT
        case reason_code::CC_LTX_WRITE_COMMITTED_READ_PROTECTION:
            return "CC_LTX_WRITE_COMMITTED_READ_PROTECTION"sv; // NOLINT
        case reason_code::CC_OCC_READ_VERIFY:
            return "CC_OCC_READ_VERIFY"sv; // NOLINT
        case reason_code::CC_OCC_WP_VERIFY:
            return "CC_OCC_WP_VERIFY"sv; // NOLINT
        case reason_code::CC_PHANTOM_AVOIDANCE:
            return "CC_PHANTOM_AVOIDANCE"sv; //NOLINT
        case reason_code::USER_ABORT:
            return "USER_ABORT"sv; // NOLINT
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, reason_code rc) { // NOLINT
    return out << to_string_view(rc);
}

class result_info {
public:
    result_info() = default;

    result_info(reason_code rc) : reason_code_(rc) {}

    [[nodiscard]] reason_code get_reason_code() const { return reason_code_; }

    void set_reason_code(reason_code rc) { reason_code_ = rc; }


private:
    reason_code reason_code_{};
};

} // namespace shirakami