/**
 * @file concurrency_control/silo/include/local_set_scheme.h
 * @brief private scheme of transaction engine
 */

#pragma once

#include <cstring>

#include "record.h"

#include "shirakami/scheme.h"

#include "glog/logging.h"

namespace shirakami {

/**
 * @brief element of write set.
 * @details copy constructor/assign operator can't be used in this class
 * in terms of performance.
 */
class write_set_obj { // NOLINT
public:
    // for insert/delete operation
    write_set_obj(Storage storage, OP_TYPE op, Record* rec_ptr)
        : op_(op), rec_ptr_(rec_ptr) {
        storage_ = {reinterpret_cast<char*>(&storage), // NOLINT
                    sizeof(storage)};
    }

    // for update/
    write_set_obj(Storage storage, std::string_view val, const OP_TYPE op,
                  Record* const rec_ptr)
        : op_(op), rec_ptr_(rec_ptr), update_value_(val) {
        storage_ = {reinterpret_cast<char*>(&storage), // NOLINT
                    sizeof(storage)};
    }

    write_set_obj(const write_set_obj& right) = delete;

    // for std::sort
    write_set_obj(write_set_obj&& right) = default;

    write_set_obj& operator=(const write_set_obj& right) = delete; // NOLINT
    // for std::sort
    write_set_obj& operator=(write_set_obj&& right) = default; // NOLINT

    bool operator<(const write_set_obj& right) const { // NOLINT
        return this->get_rec_ptr() < right.get_rec_ptr();
    }

    void get_key(std::string& out) const { rec_ptr_->get_key(out); }

    OP_TYPE& get_op() { return op_; } // NOLINT

    [[nodiscard]] const OP_TYPE& get_op() const { return op_; } // NOLINT

    Record* get_rec_ptr() { return this->rec_ptr_; } // NOLINT

    std::string_view get_storage() { return storage_; }

    void get_update_value(std::string& out) const { out = update_value_; }

    void get_insert_value(std::string& out) const {
        rec_ptr_->get_tuple().get_value(out);
    }

    [[maybe_unused]] [[nodiscard]] const Record* get_rec_ptr() const { // NOLINT
        return this->rec_ptr_;
    }

    void get_value(std::string& out) const {
        if (op_ == OP_TYPE::INSERT) {
            get_insert_value(out);
        } else if (op_ == OP_TYPE::UPDATE) {
            get_update_value(out);
        }
    }

    void reset_value(std::string_view val) {
        if (this->get_op() == OP_TYPE::UPDATE) {
            update_value_ = val;
        } else if (this->get_op() == OP_TYPE::INSERT) {
            this->get_rec_ptr()->get_tuple().get_pimpl()->set_value(val);
        } else {
            LOG(FATAL);
        }
    }

    void set_op(OP_TYPE new_op) { op_ = new_op; }

private:
    std::string storage_;
    OP_TYPE op_;
    /**
     * @brief pointer to record.
     * @details For update : ptr to existing record. For insert : ptr to new existing record.
     */
    Record* rec_ptr_;          // ptr to database
    std::string update_value_; // for update
};

class read_set_obj { // NOLINT
public:
    explicit read_set_obj(Storage storage, const Record* rec_ptr) {
        storage_ = {reinterpret_cast<char*>(&storage), // NOLINT
                    sizeof(storage)};
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
    std::string storage_{};
    Record rec_read{};
    const Record* rec_ptr{}; // ptr to database
};

} // namespace shirakami
