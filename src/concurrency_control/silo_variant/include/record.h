/**
 * @file record.h
 * @brief header about record
 */

#pragma once

#include "kvs/scheme.h"
#include "kvs/tuple.h"
#include "tid.h"

namespace shirakami::cc_silo_variant {

class Record {  // NOLINT
public:
    Record() {}  // NOLINT

    Record(std::string_view key, std::string_view val) : tuple_(key, val) {
        // init tidw
        tidw_.set_absent(true);
        tidw_.set_lock(true);
#ifdef CPR
        version_ = 0;
#endif
    }

    Record(const Record &right) = default;

    Record(Record &&right) = default;

    Record &operator=(const Record &right) = default;  // NOLINT
    Record &operator=(Record &&right) = default;       // NOLINT

    tid_word &get_tidw() { return tidw_; }  // NOLINT

    [[nodiscard]] const tid_word &get_tidw() const { return tidw_; }  // NOLINT

    Tuple &get_tuple() { return tuple_; }  // NOLINT

    [[nodiscard]] const Tuple &get_tuple() const { return tuple_; }  // NOLINT

    void set_tidw(tid_word tidw) &{ tidw_.set_obj(tidw.get_obj()); }

#if defined(CPR)

    Tuple &get_stable() { return stable_; }

    std::uint64_t get_version() { return version_; }

    void set_version(std::uint64_t new_v) { version_ = new_v; }

#endif

private:
    tid_word tidw_;
#if defined(CPR)
    std::uint64_t version_{0};
    Tuple stable_;
#endif
    Tuple tuple_;
};

}  // namespace shirakami::cc_silo_variant
