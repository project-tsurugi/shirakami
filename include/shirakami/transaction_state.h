#pragma once

#include <cstdint>
#include <iostream>
#include <string_view>

namespace shirakami {

/**
 * @brief identifier of issued transaction status handle.
 */
using TransactionStateHandle = std::uint64_t;

/**
 * @brief transaction status
 * @attention Monitoring the status will degrade the performance, so keep it 
 * to the minimum necessary.
 */
class TransactionState final {
public:
    enum class StateKind : std::int64_t {
        /**
      * @brief This status means the transaction is unknown status.
      */
        UNKNOWN = 0,
        /**
      * @brief This status means the transaction is not begun.
      */
        NOT_STARTED,
        /**
      * @brief This status means the transaction was started and waits for own epoch.
      */
        WAITING_START,
        /**
      * @brief This status means the transaction was begun.
      */
        STARTED,
        /**
      * @brief This status means the transaction was begun and must wait for a 
      * while because it can not execute this validation due to other 
      * preceding transactions.
      */
        WAITING_CC_COMMIT,
        /**
      * @brief This status means the transaction was committed from viewpoint 
      * of concurrency control and waits flushing its logs by logging protocol.
      */
        WAITING_DURABLE,
        /**
      * @brief This status means the transaction was committed from viewpoint
      * of concurrency control and logging protocol.
      */
        DURABLE
    };

    /**
     * @brief create new object (unkonwn state)
     */
    TransactionState() = default;

    explicit TransactionState(StateKind kind) noexcept : kind_(kind) {}

    /**
     * @brief destruct the object
     */
    ~TransactionState() = default;

    /**
     * @brief copy constructor
     */
    TransactionState(TransactionState const& other) = default;

    /**
     * @brief copy assignment
     */
    TransactionState& operator=(TransactionState const& other) = default;

    /**
     * @brief move constructor
     */
    TransactionState(TransactionState&& other) noexcept = default;

    /**
     * @brief move assignment
     */
    TransactionState& operator=(TransactionState&& other) noexcept = default;

    /**
     * @brief returns the transaction operation kind.
     * @return the transaction operation kind.
     */
    constexpr StateKind state_kind() const noexcept { return kind_; }

private:
    StateKind kind_{StateKind::UNKNOWN};
};

/**
 * @brief returns the label of the given enum value.
 * @param[in] value the enum value
 * @return constexpr std::string_view the corresponded label
 */
inline constexpr std::string_view
to_string_view(TransactionState::StateKind value) {
    using StateKind = TransactionState::StateKind;
    switch (value) {
        case StateKind::UNKNOWN:
            return "UNKNOWN";
        case StateKind::NOT_STARTED:
            return "NOT_STARTED";
        case StateKind::WAITING_START:
            return "WAITING_START";
        case StateKind::STARTED:
            return "STARTED";
        case StateKind::WAITING_CC_COMMIT:
            return "WAITING_CC_COMMIT";
        case StateKind::WAITING_DURABLE:
            return "WAITING_DURABLE";
        case StateKind::DURABLE:
            return "DURABLE";
    }
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream 
 * @param value the source enum value
 * @return std::ostream&  the target stream
 */
inline std::ostream& operator<<(std::ostream& out,
                                TransactionState::StateKind value) {
    return out << to_string_view(value);
}

/**
 * @brief appends object's string representation into the given stream.
 * @param out the target stream
 * @param value the source object
 * @return std::ostream& the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TransactionState value) {
    return out << to_string_view(value.state_kind());
}

} // namespace shirakami