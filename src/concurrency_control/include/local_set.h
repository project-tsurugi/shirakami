/**
 * @file concurrency_control/include/local_set.h
 */

#pragma once

#include <map>
#include <string_view>

#include "cpu.h"
#include "record.h"
#include "sequence.h"

#include "yakushima/include/kvs.h"

#include "shirakami/storage_options.h"

#include "glog/logging.h"

namespace shirakami {

class read_set_obj { // NOLINT
public:
    read_set_obj(Storage const storage, Record* const rec_ptr,
                 tid_word const tid) // NOLINT
        : storage_(storage), rec_ptr_(rec_ptr), tid_(tid) {}

    read_set_obj(const read_set_obj& right) = delete;
    read_set_obj(read_set_obj&& right) = default;

    read_set_obj& operator=(const read_set_obj& right) = delete; // NOLINT
    read_set_obj& operator=(read_set_obj&& right) = default;

    [[nodiscard]] Storage get_storage() const { return storage_; }

    [[nodiscard]] Record* get_rec_ptr() { return rec_ptr_; }

    [[nodiscard]] const Record* get_rec_ptr() const { return rec_ptr_; }

    [[nodiscard]] tid_word get_tid() const { return tid_; }

private:
    /**
     * @brief The target storage of this write.
     */
    Storage storage_{};

    /**
     * @brief Pointer to the read record in database.
     */
    Record* rec_ptr_{nullptr};

    /**
     * @brief Timestamp for optimistic read.
     */
    tid_word tid_{};
};

class write_set_obj { // NOLINT
public:
    // for update / upsert / insert
    write_set_obj(Storage const storage, OP_TYPE const op,
                  Record* const rec_ptr, std::string_view const val)
        : storage_(storage), op_(op), rec_ptr_(rec_ptr), val_(val) {
        if (op == OP_TYPE::DELETE) { LOG(ERROR) << "programming error"; }
    }

    // for delete
    write_set_obj(Storage const storage, OP_TYPE const op,
                  Record* const rec_ptr)
        : storage_(storage), op_(op), rec_ptr_(rec_ptr) {
        if (op != OP_TYPE::DELETE) { LOG(ERROR) << "programming error"; }
    }

    write_set_obj(const write_set_obj& right) = delete;
    write_set_obj(write_set_obj&& right) = default;

    write_set_obj& operator=(const write_set_obj& right) = delete;
    write_set_obj& operator=(write_set_obj&& right) = default;

    bool operator<(const write_set_obj& right) const {
        return get_rec_ptr() < right.get_rec_ptr();
    }

    bool operator==(const write_set_obj& right) const {
        return storage_ == right.storage_ && op_ == right.op_ &&
               rec_ptr_ == right.rec_ptr_ && val_ == right.val_;
    }

    void get_key(std::string& out) const { rec_ptr_->get_key(out); }

    [[nodiscard]] OP_TYPE get_op() const { return op_; }

    [[nodiscard]] Record* get_rec_ptr() const { return rec_ptr_; }

    [[nodiscard]] Storage get_storage() const { return storage_; }

    void get_value(std::string& out) const {
        if (get_op() == OP_TYPE::INSERT || get_op() == OP_TYPE::UPSERT ||
            get_op() == OP_TYPE::UPDATE) {
            out = val_;
            return;
        }
        if (get_op() == OP_TYPE::DELETE) { return; }
        LOG(ERROR) << "programming error";
    }

    std::string_view get_value_view() { return val_; }

    void set_op(OP_TYPE op) { op_ = op; }

    void set_rec_ptr(Record* rec_ptr) { rec_ptr_ = rec_ptr; }

    /**
     * @brief set value
     * @details It is for twice update in the same transaction.
     */
    void set_val(std::string_view const val) { val_ = val; }

private:
    /**
     * @brief The target storage of this write.
     */
    Storage storage_{};
    /**
     * @brief The operation type of this write.
     */
    OP_TYPE op_{};
    /**
     * @brief Pointer to record.
     * @details For update, this is a pointer to existing record.
     * For insert, this is a pointer to new existing record.
     */
    Record* rec_ptr_{};
    /**
     * @brief Update cache for update.
     */
    std::string val_{}; // value for update
};

class local_write_set {
public:
    /**
     * @brief container type for short tx.
     */
    using cont_for_occ_type = std::vector<write_set_obj>;
    /**
     * @brief container type for batch (long tx).
     */
    using cont_for_bt_type = std::map<Record*, write_set_obj>;

    /**
     * @brief clear containers.
     */
    void clear() {
        cont_for_occ_.clear();
        cont_for_bt_.clear();
    }

    Status erase(write_set_obj* wso);

    [[nodiscard]] bool get_for_batch() const { return for_batch_; }

    cont_for_bt_type& get_ref_cont_for_bt() { return cont_for_bt_; }

    cont_for_occ_type& get_ref_cont_for_occ() { return cont_for_occ_; }

    /**
     * @brief push an element to local write set.
     */
    void push(write_set_obj&& elem);

    /**
     * @brief check whether it already executed write operation.
     * @param[in] rec_ptr the target record.
     * @return the pointer of element. If it is nullptr, it is not found.
     */
    write_set_obj* search(Record const* rec_ptr);

    void set_for_batch(bool const tf) { for_batch_ = tf; }

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
    bool for_batch_{false};
    /**
     * @brief container for batch.
     */
    cont_for_bt_type cont_for_bt_;
    /**
     * @brief container for short tx.
     */
    cont_for_occ_type cont_for_occ_;
};

class local_sequence_set {
public:
    using container_type =
            std::map<SequenceId, std::tuple<SequenceVersion, SequenceValue>>;

    void clear() { set_.clear(); }
    container_type& set() { return set_; }

    Status push(SequenceId id, SequenceVersion version, SequenceValue value);

private:
    container_type set_;
};

/**
 * @brief It is used for registering read information.
 * 
 */
class local_read_set_for_ltx {
public:
    using cont_type = std::set<Record*>;

    void clear() { set_.clear(); }

    void push(Record* rec) { set_.insert(rec); }

    // getter
    cont_type& set() { return set_; }

private:
    cont_type set_;
};

} // namespace shirakami