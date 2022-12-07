#pragma once

namespace shirakami {

/**
 * @brief 128 bit transaction identifier.
 */
class tx_id {
public:
    // getter
    [[nodiscard]] std::uint32_t get_higher_info() const { return higher_info_; }

    [[nodiscard]] std::uint32_t get_session_id() const { return session_id_; }

    [[nodiscard]] std::uint64_t get_lower_info() const { return lower_info_; }

    // setter
    void set_higher_info(std::uint32_t num) { higher_info_ = num; }

    void set_session_id(std::uint32_t num) { session_id_ = num; }

    void set_lower_info(std::uint64_t num) { lower_info_ = num; }

private:
    /**
     * @brief The number of times the lower information has circulated.
     */
    std::uint32_t higher_info_;

    /**
     * @brief The session id which executed this transaction.
     */
    std::uint32_t session_id_;

    /**
     * @brief The lower information made by session id and tx counter of the 
     * session.
     */
    std::uint64_t lower_info_;
};

} // namespace shirakami