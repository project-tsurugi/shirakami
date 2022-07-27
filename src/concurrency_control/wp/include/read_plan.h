#pragma once

#include <shared_mutex>
#include <vector>

#include "shirakami/transaction_options.h"

namespace shirakami {

/**
 * @brief read plan
 * @details This is had at page set metadata. This object shows what transactions
 * read or not read the page set.
 */
class read_plan {
public:
    using list_type = std::set<std::size_t>;

    read_plan() = default;

    read_plan(list_type const& pl, list_type const& nl)
        : positive_list_(pl), negative_list_(nl) {}

    void update_positive_list(std::size_t const tx_id) {
        std::lock_guard<std::shared_mutex> lk{mtx_positive_list_};
        positive_list_.insert(tx_id);
    }

    void update_negative_list(std::size_t const tx_id) {
        std::lock_guard<std::shared_mutex> lk{mtx_negative_list_};
        negative_list_.insert(tx_id);
    }

private:
    /**
     * @brief mutex for positive list.
     */
    std::shared_mutex mtx_positive_list_;

    /**
     * @brief mutex for negative list.
     */
    std::shared_mutex mtx_negative_list_;

    /**
     * @brief positive list. This list includes the transaction information which
     * may read the page set.
     */
    list_type positive_list_;

    /**
     * @brief negative list. This list includes the transaction information which
     * should not read the page set.
     */
    list_type negative_list_;
};

/**
 * @brief Set the read plan object at each storage.
 * @param[in] tx_id the transaction id.
 * @param[in] ra read area of this transaction.
 * @return Status::OK success
 * @return Status::WARN_STORAGE_NOT_FOUND
 */
extern Status set_read_plans(std::size_t tx_id,
                             transaction_options::read_area const& ra);

} // namespace shirakami