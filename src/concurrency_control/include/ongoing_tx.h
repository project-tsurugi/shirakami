/**
 * @file concurrency_control/include/ongoing_tx.h
 */

#pragma once

#include <atomic>
#include <mutex>
#include <set>
#include <shared_mutex>

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/scheme.h"

#include "glog/logging.h"

namespace shirakami {

class ongoing_tx {
public:
    /**
      * @brief tx_info_elem_type. first is epoch, second is batch id.
      * 
      */
    using tx_info_elem_type = std::tuple<epoch::epoch_t, std::size_t>;
    static constexpr std::size_t index_epoch = 0;
    static constexpr std::size_t index_id = 1;
    using tx_info_type = std::vector<tx_info_elem_type>;

    /**
     * @brief Change epoch of @a tx_id for @a new_ep.
     * @pre The lock of ongoing_tx was acquired outside from this tx.
     * @param[in] tx_id The target tx id.
     * @param[in] new_ep The target epoch of the tx.
     * @return Status::OK success
     * @return Status::WARN_NOT_FOUND fail for not existing @a tx_id.
     */
    static Status change_epoch_without_lock(std::size_t tx_id,
                                            epoch::epoch_t new_ep);

    static bool exist_id(std::size_t id);

    /**
     * @brief 
     * 
     * @param[in] ti This tx's session information.
     * @return true It exists transactions to wait.
     * @return false It doesn't exist transactions to wait.
     */
    static bool exist_wait_for(session* ti);

    /**
     * @brief Get the lowest epoch
     * 
     * @return std::size_t if ret equals to 0, tx_info_ is empty. Otherwise, ret is lowest epoch.
     */
    static epoch::epoch_t get_lowest_epoch() {
        return lowest_epoch_.load(std::memory_order_acquire);
    }

    /**
     * @brief Get the mtx object
     * @return std::shared_mutex& 
     */
    static std::shared_mutex& get_mtx() { return mtx_; }

    /**
     * @brief Get the tx info object
     * @details for developping. not use for core codes.
     * @return tx_info_type& 
     */
    static tx_info_type& get_tx_info() { return tx_info_; }

    static void push(tx_info_elem_type ti);

    static void push_bringing_lock(tx_info_elem_type ti);

    static void remove_id(std::size_t id);

    static void set_lowest_epoch(epoch::epoch_t ep) {
        lowest_epoch_.store(ep, std::memory_order_release);
    }

private:
    /**
     * @brief This is mutex for tx_info_;
     */
    static inline std::shared_mutex mtx_; // NOLINT
    /**
     * @brief register info of running long tx's epoch and id.
     */
    static inline tx_info_type tx_info_; // NOLINT
    /**
     * @brief lowest epoch of running long tx
     * @details This variables is read by short tx and long tx both. This is 
     * used by them for read_by gc.
     */
    static inline std::atomic<epoch::epoch_t> lowest_epoch_{0}; // NOLINT
};

} // namespace shirakami