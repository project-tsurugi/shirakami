/**
 * @file src/concurrency_control/include/local_set.h
 */

#pragma once

#include <map>
#include <utility>

#include "concurrency_control/include/local_set_scheme.h"
#include "concurrency_control/include/record.h"

#include "yakushima/include/kvs.h"

namespace shirakami {

/**
 * @brief For local write set
 */
class local_write_set {
public:
    using cont_for_ol_type = std::vector<write_set_obj>;
    using cont_for_bt_type = std::map<Record*, write_set_obj>;

    void clear() {
        cont_for_ol_.clear();
        cont_for_bt_.clear();
    }

    [[maybe_unused]] void display_write_set();

    cont_for_bt_type& get_cont_for_bt();

    cont_for_ol_type& get_cont_for_ol();

    bool get_for_batch() { return for_batch_; }

    void push(write_set_obj&& elem);

    /**
     * @brief Remove inserted records of write set from masstree.
     *
     * Insert operation inserts records to masstree in read phase.
     * If the transaction is aborted, the records exists for ever with absent
     * state. So it needs to remove the inserted records of write set from
     * masstree at abort.
     * @pre This function is called at abort.
     */
    void remove_inserted_records_from_yakushima(
            shirakami::Token shirakami_token,
            yakushima::Token yakushima_token);

    /**
     * @brief check whether it already executed write operation.
     * @param[in] rec_ptr the target record.
     * @return the pointer of element. If it is nullptr, it is not found.
     */
    write_set_obj* search(Record* rec_ptr);

    void set_for_batch(bool tf) { for_batch_ = tf; }

    void sort_if_ol();

    /**
     * @brief unlock records in write set.
     *
     * This function unlocked all records in write set absolutely.
     * So it has a pre-condition.
     * @pre It has locked all records in write set.
     * @return void
     */
    void unlock();

    /**
     * @brief unlock write set object between @a begin and @a begin + num.
     * @param [in] num The number of removing.
     * @pre It already locked write set between @a begin and @a end.
     * @return void
     */
    void unlock(std::size_t num);

private:
    /**
     * @brief A flag that identifies whether the container is for batch processing or online processing.
     */
    bool for_batch_{false};
    cont_for_bt_type cont_for_bt_;
    cont_for_ol_type cont_for_ol_;
};

} // namespace shirakami