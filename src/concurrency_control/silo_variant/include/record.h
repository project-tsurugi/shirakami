/**
 * @file record.h
 * @brief header about record
 */

#pragma once

#include "cpu.h"
#include "tid.h"
#include "tuple_local.h"

#include "kvs/scheme.h"
#include "kvs/tuple.h"

namespace shirakami::cc_silo_variant {

class alignas(CACHE_LINE_SIZE) Record {  // NOLINT
public:
    Record() {}  // NOLINT

    Record(std::string_view key, std::string_view val) : tuple_(key, val) {
        // init tidw
        tidw_.set_latest(true);
        tidw_.set_absent(true);
        tidw_.set_lock(true);
#ifdef CPR
        stable_tidw_.get_obj() = 0;
        version_ = 0;
        checkpointed_ = false;
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

    tid_word &get_stable_tidw() { return stable_tidw_; }

    std::uint64_t get_version() { return version_; }

    bool get_checkpointed() { return checkpointed_; }

    void set_version(std::uint64_t new_v) { version_ = new_v; }

    void set_stable_tidw(tid_word new_tid) { stable_tidw_ = new_tid; }

#endif

private:
    tid_word tidw_;
#if defined(CPR)
    /**
     * @details Improvement from original CPR. If stable version is also latest version, it doesn't need to update
     * stable version.
     */
    tid_word stable_tidw_;
    /**
     * @pre Only lock owner can read-write this filed.
     */
    std::uint32_t version_{0};
    Tuple stable_;
    /**
     * @brief If CPR checkpointer processed, this is true.
     */
    bool checkpointed_{false};
#endif
    Tuple tuple_;
};

}  // namespace shirakami::cc_silo_variant