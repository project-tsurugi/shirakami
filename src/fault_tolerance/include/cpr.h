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

#include <msgpack.hpp>

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/record.h"

#include "shirakami/interface.h"

#include <tsl/hopscotch_map.h>

namespace shirakami::cpr {

using version_type = std::uint64_t;

using register_count_type = std::uint64_t;
constexpr register_count_type register_count_type_max = UINT64_MAX;

inline std::atomic<bool> kCheckPointThreadEnd{false};      // NOLINT
inline std::atomic<bool> kCheckPointThreadEndForce{false}; // NOLINT
inline std::atomic<bool> kCheckPointThreadJoined{false};   // NOLINT
inline std::thread kCheckPointThread;                      // NOLINT

inline std::string kCheckpointPath;    // NOLINT
inline std::string kCheckpointingPath; // NOLINT

// global variables setter / getter

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
#if defined(CPR_DIFF_HOPSCOTCH)
    using diff_upd_set_type = tsl::hopscotch_map<std::string, tsl::hopscotch_map<std::string, std::pair<tid_word, Record*>>>;
#elif defined(CPR_DIFF_UM)
    using diff_upd_set_type = std::unordered_map<std::string, std::unordered_map<std::string, std::pair<register_count_type, Record*>>>;
#elif defined(CPR_DIFF_VEC)
    using diff_upd_set_type = std::vector<std::tuple<std::string, std::string, tid_word, Record*>>;
#endif
    using diff_upd_seq_set_type = tsl::hopscotch_map<SequenceValue, std::tuple<SequenceVersion, SequenceValue>, std::hash<SequenceValue>>;
    constexpr static std::size_t reserve_num = PARAM_CPR_DIFF_SET_RESERVE_NUM;

    void clear_diff_set() {
        diff_upd_set_ar.at(0).clear();
        diff_upd_set_ar.at(1).clear();
        diff_upd_seq_set_ar.at(0).clear();
        diff_upd_seq_set_ar.at(1).clear();
    }

    bool diff_upd_set_is_empty() {
        for (auto&& each_set : diff_upd_set_ar) {
            for (auto&& elem : each_set) {
                if (!std::get<1>(elem).empty()) {
                    return false;
                }
            }
        }
        return true;
    }

    bool diff_upd_seq_set_is_empty() {
        return get_diff_upd_seq_set(0).empty() && get_diff_upd_seq_set(1).empty();
    }

    diff_upd_set_type& get_diff_upd_set();

    diff_upd_seq_set_type& get_diff_upd_seq_set();

    diff_upd_set_type& get_diff_upd_set(std::size_t index) {
        return diff_upd_set_ar.at(index);
    }

    diff_upd_seq_set_type& get_diff_upd_seq_set(std::size_t index) {
        return diff_upd_seq_set_ar.at(index);
    }

    phase get_phase() { return phase_version_.load(std::memory_order_acquire).get_phase(); } // NOLINT

    version_type get_version() { return phase_version_.load(std::memory_order_acquire).get_version(); } // NOLINT

    void reserve_diff_set() {
        diff_upd_set_ar.at(0).reserve(reserve_num);
        diff_upd_set_ar.at(1).reserve(reserve_num);
        diff_upd_seq_set_ar.at(0).reserve(reserve_num);
        diff_upd_seq_set_ar.at(1).reserve(reserve_num);
    }

    void set_phase_version(phase_version new_phase_version) {
        phase_version_.store(new_phase_version, std::memory_order_release);
    }

private:
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
     * @todo TODO. Using std :: string for the key is redundant. In the future this will be static_cast(uint64_t)(record *). 
     * In that case, two things must be considered so that the pointer is not confused.
     * 1. Does the record with the same key point to another pointer?
     * 2. Do records with different keys point to the same pointer?
     * This can be resolved so that the physical memory reuse associated with the physical deletion of records crosses the boundaries of CPR. 
     *  Since it is highly optimized, it is future work.
     */
    std::array<diff_upd_set_type, 2> diff_upd_set_ar; // NOLINT

    std::array<diff_upd_seq_set_type, 2> diff_upd_seq_set_ar; // NOLINT
    std::atomic<phase_version> phase_version_{};
};

class log_record {
public:
    log_record() = default;

    /**
     * @brief for delete operation.
     */
    log_record(std::string_view const storage, std::string_view const key) {
        storage_ = storage;
        key_ = key;
        val_.clear();
        delete_op_ = true;
    }

    log_record(std::string_view const storage, std::string_view const key, std::string_view const val) {
        storage_ = storage;
        key_ = key;
        val_ = val;
        delete_op_ = false;
    }

    [[nodiscard]] bool get_delete_op() const { return delete_op_; }

    std::string_view get_key() const { return key_; } // NOLINT

    std::string_view get_storage() const { return storage_; } // NOLINT

    std::string_view get_val() const { return val_; } // NOLINT

    MSGPACK_DEFINE(delete_op_, storage_, key_, val_);

private:
    /**
     * @details If this is true, this log means delete. If this is false, 
     * this log means update.
     */
    bool delete_op_{false};
    std::string storage_{};
    std::string key_{};
    std::string val_{};
};

class log_record_of_seq {
public:
    log_record_of_seq() = default;

    log_record_of_seq(SequenceValue key, std::tuple<SequenceVersion, SequenceValue> val) {
        key_ = key;
        val_ = val;
    }

    [[nodiscard]] SequenceValue get_id() const { return key_; }

    [[nodiscard]] std::tuple<SequenceVersion, SequenceValue> get_val() const { return val_; }

    MSGPACK_DEFINE(key_, val_);

private:
    SequenceValue key_{};
    std::tuple<SequenceVersion, SequenceValue> val_{};
};

class log_records {
public:
    void emplace_back(std::string_view const storage, std::string_view const key, std::string_view const val) {
        vec_.emplace_back(storage, key, val);
    }

    void emplace_back(std::string_view const storage, std::string_view const key) {
        vec_.emplace_back(storage, key);
    }

    void emplace_back_seq(log_record_of_seq elem) {
        vec_of_seq_.emplace_back(elem);
    }

    std::vector<log_record>& get_vec() { return vec_; } // NOLINT

    std::vector<log_record_of_seq>& get_vec_seq() { return vec_of_seq_; } // NOLINT

    MSGPACK_DEFINE(vec_, vec_of_seq_);

private:
    std::vector<log_record> vec_;
    std::vector<log_record_of_seq> vec_of_seq_;
};

// about global variables.
[[maybe_unused]] static std::string& get_checkpoint_path() { return kCheckpointPath; } // NOLINT

[[maybe_unused]] static std::string& get_checkpointing_path() { return kCheckpointingPath; } // NOLINT

[[maybe_unused]] static bool get_checkpoint_thread_end() { return kCheckPointThreadEnd.load(std::memory_order_acquire); }

[[maybe_unused]] static bool get_checkpoint_thread_end_force() { return kCheckPointThreadEndForce.load(std::memory_order_acquire); }

[[maybe_unused]] static bool get_checkpoint_thread_joined() { return kCheckPointThreadJoined.load(std::memory_order_acquire); }

[[maybe_unused]] static void join_checkpoint_thread() try {
    kCheckPointThread.join();
} catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
}

[[maybe_unused]] static void set_checkpoint_thread_end(const bool tf) {
    kCheckPointThreadEnd.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_checkpoint_thread_end_force(const bool tf) {
    kCheckPointThreadEndForce.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_checkpoint_thread_joined(const bool tf) {
    kCheckPointThreadJoined.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_checkpoint_path(std::string_view str) { kCheckpointPath.assign(str); }

[[maybe_unused]] static void set_checkpointing_path(std::string_view str) { kCheckpointingPath.assign(str); }

[[maybe_unused]] extern void wait_next_checkpoint();

/**
 * @brief This is checkpoint thread and manager of cpr.
 */
extern void checkpoint_thread();

/**
 * @brief Checkpointing for entire database.
 */
extern void checkpointing();

[[maybe_unused]] static void invoke_checkpoint_thread() {
    set_checkpoint_thread_end(false);
    set_checkpoint_thread_end_force(true);
    set_checkpoint_thread_joined(false);
    kCheckPointThread = std::thread(checkpoint_thread);
}

} // namespace shirakami::cpr
