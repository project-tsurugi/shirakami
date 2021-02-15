/**
 * @file pwal.h
 * @brief Log record class.
 * @details This source is implemented by refering the source
 * https://github.com/thawk105/ccbench.
 */

#pragma once

#include <cstdint>
#include <cstring>

#include "fileio.h"

#include "kvs/interface.h"
#include "kvs/scheme.h"

#include "concurrency_control/silo_variant/include/tid.h"

namespace shirakami::pwal {

class LogHeader {
public:
    /**
     * @brief Adds the argument to @var LogHeader::checksum_.
     */
    void add_checksum(int add);

    /**
     * @brief Computing check sum.
     * @details Compute the two's complement of the checksum.
     */
    void compute_two_complement_of_checksum();

    /**
     * @brief Gets the value of @var LogHeader::checksum_.
     */
    [[nodiscard]] unsigned int get_checksum() const;  // NOLINT

    /**
     * @brief Gets the value of @var LogHeader::log_rec_num_.
     */
    [[nodiscard]] unsigned int get_log_rec_num() const;  // NOLINT

    /**
     * @brief Adds the one to @var LogHeader::log_rec_num_.
     */
    void inc_log_rec_num();

    /**
     * @brief Initialization
     * @details Initialize members with 0.
     */
    void init();

    /**
     * @brief Sets @a LogHeader::checksum_ to the argument.
     * @param checksum
     */
    void set_checksum(unsigned int checksum);

private:
    unsigned int checksum_{};
    unsigned int log_rec_num_{};
    const unsigned int mask_full_bits_uint = 0xffffffff;
};

class LogRecord {
public:
    LogRecord() = default;

    LogRecord(const cc_silo_variant::tid_word &tid, const OP_TYPE op, const Tuple* const tuple)
            : tid_(tid), op_(op), tuple_(tuple) {}

    bool operator<(const LogRecord &right) {  // NOLINT
        return this->tid_ < right.tid_;
    }

    /**
     * @brief Compute checksum.
     */
    unsigned int compute_checksum();  // NOLINT

    cc_silo_variant::tid_word &get_tid() { return tid_; }  // NOLINT

    [[maybe_unused]] [[nodiscard]] const cc_silo_variant::tid_word &get_tid() const {  // NOLINT
        return tid_;
    }

    [[nodiscard]] const Tuple* get_tuple() const { return tuple_; }  // NOLINT

    OP_TYPE &get_op() { return op_; }  // NOLINT

    [[maybe_unused]] [[nodiscard]] const OP_TYPE &get_op() const {  // NOLINT
        return op_;
    }

    [[maybe_unused]] void set_tuple(Tuple* tuple) { this->tuple_ = tuple; }

private:
    cc_silo_variant::tid_word tid_{};
    OP_TYPE op_{OP_TYPE::NONE};
    const Tuple* tuple_{nullptr};
};

class pwal_handler {
public:

    cc_silo_variant::tid_word &get_flushed_ctid() { return flushed_ctid_; } // NOLINT

    std::vector<LogRecord> &get_log_set() { return log_set_; }  // NOLINT

    File &get_log_file() { return log_file_; }  // NOLINT

    LogHeader &get_latest_log_header() {  // NOLINT
        return latest_log_header_;
    }

    void set_flushed_ctid(const cc_silo_variant::tid_word& new_ctid) { flushed_ctid_ = new_ctid; }

private:
    File log_file_{};
    std::vector<LogRecord> log_set_{};
    LogHeader latest_log_header_{};
    cc_silo_variant::tid_word flushed_ctid_{};
};

}  // namespace shirakami::cc_silo_variant
