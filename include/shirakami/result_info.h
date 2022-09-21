#pragma once

#include <string_view>

namespace shirakami {

/**
 * @brief Reason code. This is a secondary information about result.
 */
enum class reason_code : std::int32_t {
    /**
     * @brief This reason means the result was got due to protection for 
     * committed read.
     */
    COMMITTED_READ_PROTECTION,
    /**
     * @brief Invalidation about read upper bound. This shows the lts failed to
     * forwarding due to breaking old own read.
     */
    INVALID_READ_UPPER_BOUND,
};

inline constexpr std::string_view to_string_view(reason_code rc) noexcept {
    using namespace std::string_view_literals;
    switch (rc) {
        case reason_code::COMMITTED_READ_PROTECTION:
            return "COMMITTED_READ_PROTECTION"sv; // NOLINT
        case reason_code::INVALID_READ_UPPER_BOUND:
            return "INVALID_READ_UPPER_BOUND"sv; // NOLINT
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