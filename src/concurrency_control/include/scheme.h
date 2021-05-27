/**
 * @file src/concurrency_control/include/scheme.h
 * @brief private scheme of transaction engine
 */

#pragma once

#include <cstring>

#include "record.h"
#include "scheme.h"

#include "shirakami/scheme.h"

namespace shirakami {

/**
 * @brief element of write set.
 * @details copy constructor/assign operator can't be used in this class
 * in terms of performance.
 */
class write_set_obj { // NOLINT
public:
    // for insert/delete operation
    write_set_obj(Storage storage, OP_TYPE op, Record* rec_ptr) : op_(op), rec_ptr_(rec_ptr) {
        storage_ = {reinterpret_cast<char*>(&storage), sizeof(storage)}; // NOLINT
    }

    // for update/
    write_set_obj(Storage storage, std::string_view key, std::string_view val, const OP_TYPE op,
                  Record* const rec_ptr)
        : op_(op), rec_ptr_(rec_ptr), tuple_(key, val) {
        storage_ = {reinterpret_cast<char*>(&storage), sizeof(storage)}; // NOLINT
    }

    write_set_obj(const write_set_obj& right) = delete;

    // for std::sort
    write_set_obj(write_set_obj&& right) = default;

    write_set_obj& operator=(const write_set_obj& right) = delete; // NOLINT
    // for std::sort
    write_set_obj& operator=(write_set_obj&& right) = default; // NOLINT

    bool operator<(const write_set_obj& right) const; // NOLINT

    Record* get_rec_ptr() { return this->rec_ptr_; } // NOLINT

    [[maybe_unused]] [[nodiscard]] const Record* get_rec_ptr() const { // NOLINT
        return this->rec_ptr_;
    }

    std::string_view get_storage() { return storage_; }

    /**
     * @brief get tuple ptr appropriately by operation type.
     * @return Tuple&
     */
    Tuple& get_tuple() { return get_tuple(op_); } // NOLINT

    [[maybe_unused]] [[nodiscard]] const Tuple& get_tuple() const { // NOLINT
        return get_tuple(op_);
    }

    /**
     * @brief get tuple ptr appropriately by operation type.
     * @return Tuple&
     */
    Tuple& get_tuple(const OP_TYPE op) { // NOLINT
        if (op == OP_TYPE::UPDATE) {
            return get_tuple_to_local();
        }
        // insert/delete
        return get_tuple_to_db();
    }

    /**
     * @brief get tuple ptr appropriately by operation type.
     * @return const Tuple& const
     */
    [[nodiscard]] const Tuple& get_tuple(const OP_TYPE op) const { // NOLINT
        if (op == OP_TYPE::UPDATE) {
            return get_tuple_to_local();
        }
        // insert/delete
        return get_tuple_to_db();
    }

    /**
     * @brief get tuple ptr to local write set
     * @return Tuple&
     */
    Tuple& get_tuple_to_local() { return this->tuple_; } // NOLINT

    /**
     * @brief get tuple ptr to local write set
     * @return const Tuple&
     */
    [[nodiscard]] const Tuple& get_tuple_to_local() const { // NOLINT
        return this->tuple_;
    }

    /**
     * @brief get tuple ptr to database(global)
     * @return Tuple&
     */
    Tuple& get_tuple_to_db() { return this->rec_ptr_->get_tuple(); } // NOLINT

    /**
     * @brief get tuple ptr to database(global)
     * @return const Tuple&
     */
    [[nodiscard]] const Tuple& get_tuple_to_db() const { // NOLINT
        return this->rec_ptr_->get_tuple();
    }

    OP_TYPE& get_op() { return op_; } // NOLINT

    [[nodiscard]] const OP_TYPE& get_op() const { return op_; } // NOLINT

    void reset_tuple_value(std::string_view val);

    void set_op(OP_TYPE new_op) {
        op_ = new_op;
    }

private:
    std::string storage_;
    OP_TYPE op_;
    /**
     * @brief pointer to record.
     * @details For update : ptr to existing record. For insert : ptr to new existing record.
     */
    Record* rec_ptr_; // ptr to database
    Tuple tuple_;     // for update
};

class read_set_obj { // NOLINT
public:
    read_set_obj() { this->rec_ptr = nullptr; }

    explicit read_set_obj(Storage storage, const Record* rec_ptr) {
        storage_ = {reinterpret_cast<char*>(&storage), sizeof(storage)}; // NOLINT
        this->rec_ptr = rec_ptr;
    }

    read_set_obj(const read_set_obj& right) = delete;

    read_set_obj(read_set_obj&& right) = default;

    read_set_obj& operator=(const read_set_obj& right) = delete; // NOLINT
    read_set_obj& operator=(read_set_obj&& right) = default;

    Record& get_rec_read() { return rec_read; } // NOLINT

    [[nodiscard]] const Record& get_rec_read() const { // NOLINT
        return rec_read;
    }

    const Record* get_rec_ptr() { return rec_ptr; } // NOLINT

    [[maybe_unused]] [[nodiscard]] const Record* get_rec_ptr() const { // NOLINT
        return rec_ptr;
    }

    [[nodiscard]] std::string_view get_storage() { return storage_; }

private:
    std::string storage_;
    Record rec_read{};
    const Record* rec_ptr{}; // ptr to database
};

} // namespace shirakami
