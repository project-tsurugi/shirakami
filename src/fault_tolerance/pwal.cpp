/**
 * @file log.cpp
 * @brief implement about log
 */

#include "fault_tolerance/include/pwal_test.h"

#include "tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant {

void Log::LogHeader::init() {
    checksum_ = 0;
    log_rec_num_ = 0;
}

void Log::LogHeader::compute_two_complement_of_checksum() {
    checksum_ ^= mask_full_bits_uint;
    ++checksum_;
}

unsigned int Log::LogHeader::get_checksum() const {  // NOLINT
    return checksum_;
}

void Log::LogHeader::add_checksum(const int add) { checksum_ += add; }

unsigned int Log::LogHeader::get_log_rec_num() const {  // NOLINT
    return log_rec_num_;
}

void Log::LogHeader::inc_log_rec_num() { ++this->log_rec_num_; }

void Log::LogHeader::set_checksum(unsigned int checksum) {
    this->checksum_ = checksum;
}

unsigned int Log::LogRecord::compute_checksum() {  // NOLINT
    // compute checksum
    // tid_word
    unsigned int chkSum = 0;
    const char* charitr = reinterpret_cast<char*>(this);  // NOLINT
    for (std::size_t i = 0; i < sizeof(tid_word); ++i) {
        chkSum += (*charitr);
        ++charitr;  // NOLINT
    }

    // OP_TYPE
    chkSum += static_cast<decltype(chkSum)>(op_);

    // key_length
    std::string_view key_view = tuple_->get_key();
    std::size_t key_length = key_view.size();
    charitr = reinterpret_cast<char*>(&(key_length));  // NOLINT
    for (std::size_t i = 0; i < sizeof(std::size_t); ++i) {
        chkSum += (*charitr);
        ++charitr;  // NOLINT
    }

    // key_body
    charitr = key_view.data();
    for (std::size_t i = 0; i < key_view.size(); ++i) {
        chkSum += (*charitr);
        ++charitr;  // NOLINT
    }

    // value_length
    std::string_view value_view = tuple_->get_value();
    std::size_t value_length = value_view.size();
    charitr = reinterpret_cast<char*>(&(value_length));  // NOLINT
    for (std::size_t i = 0; i < sizeof(std::size_t); ++i) {
        chkSum += (*charitr);
        ++charitr;  // NOLINT
    }

    // value_body
    charitr = value_view.data();
    for (std::size_t i = 0; i < value_view.size(); ++i) {
        chkSum += (*charitr);
        ++charitr;  // NOLINT
    }

    return chkSum;
}


}  // namespace shirakami::cc_silo_variant

