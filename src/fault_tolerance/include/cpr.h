/**
 * @file cpr.h
 * @details cpr is concurrent prefix recovery
 * ( https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf ).
 */

#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <tuple>

#include "logger.h"

#include "concurrency_control/silo_variant/include/epoch.h"
#include "concurrency_control/silo_variant/include/record.h"

#include "msgpack-c/include/msgpack.hpp"

#include <tsl/hopscotch_map.h>

namespace shirakami::cpr {

using version_type = std::uint64_t;

#ifndef PARAM_CPR_USE_FULL_SCAN
using register_count_type = std::uint64_t;
constexpr register_count_type register_count_type_max = UINT64_MAX;
#endif

inline std::atomic<bool> kCheckPointThreadEnd{false}; // NOLINT
inline std::thread kCheckPointThread;                 // NOLINT
inline std::string kCheckpointingPath;                // NOLINT
inline std::string kCheckpointPath;                   // NOLINT

#ifndef PARAM_CPR_USE_FULL_SCAN
inline std::array<std::atomic<register_count_type>, 2> kRegisterCount{}; // NOLINT
#endif

enum class phase : char {
    REST = 0,
    IN_PROGRESS,
    WAIT_FLUSH,
};

class phase_version {
public:
    phase get_phase() { return phase_; } // NOLINT

    [[nodiscard]] version_type get_version() const { return version_; } // NOLINT

    void inc_version() { version_ += 1; }

    void set_phase(phase new_phase) { phase_ = new_phase; }

    void set_version(version_type new_version) { version_ = new_version; }

private:
    phase phase_ : 8;
    version_type version_ : 56;
};

/**
 * @brief Shared global phase version.
 * @todo Measures for round-trip of version counter.
 */
class global_phase_version {
public:
    static phase_version get_gpv() { return body.load(std::memory_order_acquire); } // NOLINT

    static void inc_version() {
        phase_version new_body = body.load(std::memory_order_acquire);
        new_body.inc_version();
        body.store(new_body, std::memory_order_release);
    }

    static void init() {
        body.store(phase_version(), std::memory_order_release);
    }

    static void set_gp(phase new_phase) {
        phase_version new_body = body.load(std::memory_order_acquire);
        new_body.set_phase(new_phase);
        body.store(new_body, std::memory_order_release);
    }

    static void set_rest() {
        phase_version new_body = body.load(std::memory_order_acquire);
        new_body.set_phase(phase::REST);
        new_body.set_version(new_body.get_version() + 1);
        body.store(new_body, std::memory_order_release);
    }

private:
    static inline std::atomic<phase_version> body{phase_version()}; // NOLINT
};

/**
 * @brief This object is had by worker thread for concurrent prefix recovery.
 */
class cpr_local_handler {
public:
#ifndef PARAM_CPR_USE_FULL_SCAN
    static void aggregate_diff_update_set(tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>& aggregate_buf);

    tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>& get_diff_update_set();

    tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>& get_diff_update_set(std::size_t index) {
        return diff_update_set.at(index);
    }

#endif

    phase get_phase() { return phase_version_.load(std::memory_order_acquire).get_phase(); } // NOLINT

    version_type get_version() { return phase_version_.load(std::memory_order_acquire).get_version(); } // NOLINT

    void set_phase_version(phase_version new_phase_version) {
        phase_version_.store(new_phase_version, std::memory_order_release);
    }

private:
#ifndef PARAM_CPR_USE_FULL_SCAN
    /**
     * @brief A set of keys updated by this worker thread.
     * @details The CPR manager aggregates this set of each worker thread and considers it a delta update.
     * The element with index 0 is used under the following conditions.
     * -When version is even and rest phase.
     * -When version is odd and not rest phase.
     * The element of index 1 is used under the following conditions.
     * -When version is even and not rest phase.
     * -When version is odd and rest phase.
     * Clearing the element is issued by the CPR manager.
     * The pointer of pair.second should be set by nullptr if the record is deleted.
     * The reason will be described. 
     * The operation of deleting a record inserted in the same CPR logical boundary partition can occur 
     * multiple times in the same partition.
     * If the record corresponding to the key has already been registered, 
     * if the information is to be deleted by overwriting, 
     * the part corresponding to the information must be searched from the update history of all worker 
     * threads and the cancellation operation must be performed.
     * When the CPR manager collects information on all workers, 
     * it is necessary to specify the order relationship for the same record. 
     * That is why pair.first is defined.
     * @todo TODO. Using std :: string for the key is redundant. In the future this will be static_cast <uint64_t> (record *). 
     * In that case, two things must be considered so that the pointer is not confused.
     * 1. Does the record with the same key point to another pointer?
     * 2. Do records with different keys point to the same pointer?
     * This can be resolved so that the physical memory reuse associated with the physical deletion of records crosses the boundaries of CPR. 
     *  Since it is highly optimized, it is future work.
     */
    std::array<tsl::hopscotch_map<std::string, std::pair<register_count_type, Record*>>, 2> diff_update_set; // NOLINT
#endif
    std::atomic<phase_version> phase_version_{};
};

class log_record {
public:
    log_record() = default;

    log_record(std::string_view key, std::string_view val) {
        key_ = key;
        val_ = val;
    }

    std::string_view get_key() { return key_; } // NOLINT

    std::string_view get_val() { return val_; } // NOLINT

    MSGPACK_DEFINE(key_, val_);

private:
    std::string key_;
    std::string val_;
};

class log_records {
public:
    void emplace_back(std::string_view key, std::string_view val) {
        vec_.emplace_back(key, val);
    }

    std::vector<log_record>& get_vec() { return vec_; } // NOLINT

    MSGPACK_DEFINE(vec_);

private:
    std::vector<log_record> vec_;
};

/**
 * @brief This is checkpoint thread and manager of cpr.
 */
extern void checkpoint_thread();

/**
 * @brief Checkpointing for entire database.
 */
extern void checkpointing();

[[maybe_unused]] static std::string& get_checkpoint_path() { return kCheckpointPath; } // NOLINT

[[maybe_unused]] static std::string& get_checkpointing_path() { return kCheckpointingPath; } // NOLINT

[[maybe_unused]] static void invoke_checkpoint_thread() {
    kCheckPointThreadEnd.store(false, std::memory_order_release);
    kCheckPointThread = std::thread(checkpoint_thread);
}

[[maybe_unused]] static void join_checkpoint_thread() { kCheckPointThread.join(); }

[[maybe_unused]] static void set_checkpoint_thread_end(const bool tf) {
    kCheckPointThreadEnd.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_checkpoint_path(std::string_view str) { kCheckpointPath.assign(str); }

[[maybe_unused]] static void set_checkpointing_path(std::string_view str) { kCheckpointingPath.assign(str); }

[[maybe_unused]] extern void wait_next_checkpoint();

#ifndef PARAM_CPR_USE_FULL_SCAN

[[maybe_unused]] static register_count_type fetch_add_register_count(std::size_t index) {
    register_count_type ret = kRegisterCount.at(index).fetch_add(1);
    if (ret == register_count_type_max) {
        shirakami::logger::shirakami_logger->debug("wrap round error");
        exit(1);
        /**
         * It is unlikely that there will be as many writes as the maximum number of data types at the checkpoint interval.
         * If so, it is better to set the checkpoint interval shorter than it is now.
         */
    }
    return ret;
}

#endif

} // namespace shirakami::cpr
