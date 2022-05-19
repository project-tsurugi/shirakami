#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <map>
#include <shared_mutex>
#include <string_view>
#include <utility>

#include "shirakami/scheme.h"
namespace shirakami {

/**
 * @brief identifier of issued transaction status handle.
 */
using TxStateHandle = std::uint64_t;

constexpr static TxStateHandle undefined_handle = 0;

/**
 * @brief Transaction status
 * @details A mechanism for monitoring various states of a running transaction.
 * @attention Monitoring the status will degrade the performance, so keep it 
 * to the minimum necessary.
 */
class TxState final {
public:
    using handle_container_type = std::map<TxStateHandle, TxState>;
    static constexpr TxStateHandle handle_initial_value = 1;

    /**
     * @brief A various states of a running transaction.
     * @details Details of state transition is discribed at 
     * shirakami/docs/transaction_state.md
     */
    enum class StateKind : std::int64_t {
        /**
          * @brief This status means the transaction is unknown status.
          */
        UNKNOWN = 0,
        /**
          * @brief This status means the transaction was started and waits for 
          * own epoch.
          */
        WAITING_START,
        /**
          * @brief This status means the transaction was begun.
          */
        STARTED,
        /**
          * @brief This status means the transaction was begun and is waited 
          * for commit api call or must wait for a 
          * while because it can not execute this validation due to other 
          * preceding transactions.
          */
        WAITING_CC_COMMIT,
        /**
         * @brief This status means that the user can call commit api for the 
         * transaction.
         */
        COMMITTABLE,
        /**
         * @brief This status means that the transaction was aborted by api call.
         */
        ABORTED,
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
    TxState() = default;

    explicit TxState(StateKind kind) noexcept : kind_(kind) {}

    /**
     * @brief destruct the object
     */
    ~TxState() = default;

    /**
     * @brief copy constructor
     */
    TxState(TxState const& other) = default;

    /**
     * @brief copy assignment
     */
    TxState& operator=(TxState const& other) = default;

    /**
     * @brief move constructor
     */
    TxState(TxState&& other) noexcept = default;

    /**
     * @brief move assignment
     */
    TxState& operator=(TxState&& other) noexcept = default;

    void clear() {
        set_kind(StateKind::UNKNOWN);
        set_durable_epoch(0);
        set_serial_epoch(0);
    }

    static Status find_and_erase_tx_state(TxStateHandle hd, Token& token) {
        {
            std::lock_guard<std::shared_mutex> lk{mtx_hc_};
            auto itr = handle_container_.find(hd);
            if (itr != handle_container_.end()) {
                token = itr->second.get_token();
                handle_container_.erase(itr);
            } else {
                return Status::WARN_INVALID_HANDLE;
            }
        }
        {
            std::unique_lock<std::mutex> lk{mtx_reuse_ctr_container_};
            reuse_ctr_container_.emplace_back(hd);
        }
        return Status::OK;
    }

    static Status find_and_get_tx_state(TxStateHandle hd, TxState& out) {
        std::shared_lock<std::shared_mutex> lk{mtx_hc_};
        if (handle_container_.find(hd) != handle_container_.end()) {
            out = (*handle_container_.find(hd)).second;
            return Status::OK;
        }
        return Status::WARN_INVALID_HANDLE;
    }

    static void init() {
        handle_ctr_.store(handle_initial_value, std::memory_order_release);
    }

    static void insert_tx_state(TxStateHandle hd) {
        std::lock_guard<std::shared_mutex> lk{mtx_hc_};
        handle_container_.insert(std::make_pair(TxStateHandle(hd), TxState()));
    }

    static std::atomic<TxStateHandle>& get_handle_ctr() { return handle_ctr_; }

    static handle_container_type& get_handle_container() {
        return handle_container_;
    }

    static TxStateHandle get_new_handle_ctr() {
        {
            std::unique_lock<std::mutex> lk{mtx_reuse_ctr_container_};
            if (!reuse_ctr_container_.empty()) {
                TxStateHandle ret = reuse_ctr_container_.back();
                reuse_ctr_container_.pop_back();
                return ret;
            }
        }
        return handle_ctr_.fetch_add(1);
    }

    [[nodiscard]] std::uint64_t get_durable_epoch() const {
        return durable_epoch_;
    }

    [[nodiscard]] std::uint64_t get_serial_epoch() const {
        return serial_epoch_;
    }

    [[nodiscard]] Token get_token() { return token_; }

    static TxState& get_tx_state(TxStateHandle hd) {
        std::shared_lock<std::shared_mutex> lk{mtx_hc_};
        return handle_container_.at(hd);
    }

    /**
     * @brief returns the transaction operation kind.
     * @return the transaction operation kind.
     */
    [[nodiscard]] constexpr StateKind state_kind() const noexcept {
        return kind_;
    }

    void set_kind(StateKind kd) { kind_ = kd; }

    void set_durable_epoch(std::uint64_t epoch) { durable_epoch_ = epoch; }

    void set_serial_epoch(std::uint64_t epoch) { serial_epoch_ = epoch; }

    void set_token(Token token) { token_ = token; }

private:
    StateKind kind_{StateKind::UNKNOWN};
    /**
     * @brief serial epoch info
     * @details @a serial_epoch_ shows long tx's epoch. It is used for long tx 
     * only.
     */
    std::uint64_t serial_epoch_{0};
    /**
     * @brief durable epoch info
     * @details @a durable_epoch_ shows short and long tx's durable epoch. It is
     * used for decision whether the tx is durable.
     */
    std::uint64_t durable_epoch_{0};
    /**
     * @brief session info
     * @details 1: When it releases handle, the tx which has the handle can't find 
     * that actions. So the tx may execute updating status object bia its 
     * infomation. Then, it occurs heap-use-after-free etc... So it needs 
     * interaction between status object and session.
     * 2: There is a case which it needs long tx's id.
     */
    Token token_{};
    static inline std::atomic<TxStateHandle> handle_ctr_{1};       // NOLINT
    static inline handle_container_type handle_container_;         // NOLINT
    static inline std::shared_mutex mtx_hc_;                       // NOLINT
    static inline std::vector<TxStateHandle> reuse_ctr_container_; // NOLINT
    static inline std::mutex mtx_reuse_ctr_container_;             // NOLINT
};

/**
 * @brief returns the label of the given enum value.
 * @param[in] value the enum value
 * @return constexpr std::string_view the corresponded label
 */
inline constexpr std::string_view to_string_view(TxState::StateKind value) {
    using StateKind = TxState::StateKind;
    switch (value) {
        case StateKind::UNKNOWN:
            return "UNKNOWN";
        case StateKind::WAITING_START:
            return "WAITING_START";
        case StateKind::STARTED:
            return "STARTED";
        case StateKind::WAITING_CC_COMMIT:
            return "WAITING_CC_COMMIT";
        case StateKind::COMMITTABLE:
            return "COMMITTABLE";
        case StateKind::ABORTED:
            return "ABORTED";
        case StateKind::WAITING_DURABLE:
            return "WAITING_DURABLE";
        case StateKind::DURABLE:
            return "DURABLE";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream 
 * @param value the source enum value
 * @return std::ostream&  the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TxState::StateKind value) {
    return out << to_string_view(value);
}

/**
 * @brief appends object's string representation into the given stream.
 * @param out the target stream
 * @param value the source object
 * @return std::ostream& the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TxState value) {
    return out << to_string_view(value.state_kind());
}

} // namespace shirakami