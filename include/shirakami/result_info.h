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
     * @brief Abort due to phantom avoidance function in the ltx.
     */
    CC_LTX_PHANTOM_AVOIDANCE,
    /**
     * @brief violation of read area at tx_begin.
     */
    CC_LTX_READ_AREA_VIOLATION,
    /**
     * @brief The low priority ltx found high priority ltx's write preserve, 
     * and tried forwarding at commit phase but the forwarding break old own 
     * read.
     */
    CC_LTX_READ_UPPER_BOUND_VIOLATION,
    /**
     * @brief Protecting committed transactional read operation from this ltx's
     *  write operation.
     */
    CC_LTX_WRITE_COMMITTED_READ_PROTECTION,
    /**
     * @brief Occ tx detected write preserve of ltx.
     */
    CC_OCC_WP_VERIFY,
    /**
     * @brief Occ tx failed read validation due to overwrite.
     */
    CC_OCC_READ_VERIFY,
    /**
     * @brief Abort due to phantom avoidance function in the occ transaction.
     */
    CC_OCC_PHANTOM_AVOIDANCE,
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
        case reason_code::CC_LTX_PHANTOM_AVOIDANCE:
            return "CC_LTX_PHANTOM_AVOIDANCE"sv; //NOLINT
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
        case reason_code::CC_OCC_PHANTOM_AVOIDANCE:
            return "CC_OCC_PHANTOM_AVOIDANCE"sv; //NOLINT
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

    explicit result_info(reason_code rc) : reason_code_(rc) {}

    // start: getter / setter

    [[nodiscard]] reason_code get_reason_code() const { return reason_code_; }

    void set_reason_code(reason_code rc) {
        if (rc == reason_code::UNKNOWN) { set_key(""); }
        reason_code_ = rc;
    }

    [[nodiscard]] std::string_view get_key() const { return key_; }

    void set_key(std::string_view key) { key_ = key; }

    // end: getter / setter
private:
    reason_code reason_code_{};

    /**
     * @brief The reason key. reason_code::KVS_DELETE, KVS_INSERT, KVS_UPDATE, 
     * CC_LTX_PHANTOM_AVOIDANCE, CC_LTX_WRITE_COMMITTED_READ_PROTECTION, 
     * CC_OCC_READ_VERIFY log this information.
     * 
     */
    std::string key_{};
};

} // namespace shirakami