#pragma once

#include <cstdint>

namespace shirakami {

/**
 * @brief 128 bit transaction identifier.
 */
class tx_id {
public:
    using type_higher_info = std::uint32_t;
    using type_session_id = std::uint32_t;
    using type_lower_info = std::uint64_t;

    // getter
    [[nodiscard]] type_higher_info get_higher_info() const {
        return higher_info_;
    }

    [[nodiscard]] type_session_id get_session_id() const { return session_id_; }

    [[nodiscard]] type_lower_info get_lower_info() const { return lower_info_; }

    // setter
    void set_higher_info(type_higher_info num) { higher_info_ = num; }

    void set_session_id(type_session_id num) { session_id_ = num; }

    void set_lower_info(type_lower_info num) { lower_info_ = num; }

    static bool is_max_lower_info(type_lower_info num) {
        return num == UINT64_MAX;
    }

private:
    /**
     * @brief The number of times the lower information has circulated.
     */
    type_higher_info higher_info_;

    /**
     * @brief The session id which executed this transaction.
     */
    type_session_id session_id_;

    /**
     * @brief The lower information made by session id and tx counter of the
     * session.
     */
    type_lower_info lower_info_;
};

} // namespace shirakami
