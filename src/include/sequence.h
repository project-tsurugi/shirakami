/**
 * @file src/include/sequence.h
 */

#pragma once

#include <map>
#include <mutex>

#include <glog/logging.h>

#include "shirakami/interface.h"

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#endif


namespace shirakami {

class sequence_map {
public:
    /**
     * @details The first of std::pair<std::pair<SequenceVersion, SequenceValue>, 
     * std::pair<SequenceVersion, SequenceValue>> is not durable, and the second of that is durable.
     */
#if defined(CPR)
    using value_type = std::tuple<std::tuple<SequenceVersion, SequenceValue>, std::tuple<SequenceVersion, SequenceValue>, cpr::version_type>;
    static constexpr std::size_t volatile_pos{0};
    static constexpr std::size_t durable_pos{1};
    static constexpr std::tuple<SequenceVersion, SequenceValue> initial_value{0, 0};
    static constexpr std::tuple<SequenceVersion, SequenceValue> non_exist_value{SIZE_MAX, 0};
    static constexpr std::size_t cpr_version_pos{2};
    static constexpr value_type non_exist_map_value{non_exist_value, non_exist_value, 0};
#else
    using value_type = std::tuple<SequenceVersion, SequenceValue>;
    static constexpr std::tuple<SequenceVersion, SequenceValue> initial_value{0, 0};
    static constexpr std::tuple<SequenceVersion, SequenceValue> non_exist_value{SIZE_MAX, 0};
#endif
    static constexpr std::size_t version_pos{0};
    static constexpr std::size_t value_pos{1};

    /**
     * @pre It is called by create_sequence function.
     */
    static void create_initial_value(SequenceId id) {
        std::unique_lock lock{sequence_map::get_sm_mtx()};
#if defined(CPR)
        sequence_map::get_sm()[id] = sequence_map::value_type{sequence_map::initial_value, sequence_map::non_exist_value, 0};
#else
        sequence_map::get_sm()[id] = sequence_map::value_type{sequence_map::initial_value};
#endif
    }

    static std::map<SequenceId, value_type>& get_sm() {
        return sm_;
    }

    static std::mutex& get_sm_mtx() {
        return sm_mtx_;
    }

    /**
     * @pre It has acquired lock.
     * @param[in] id 
     * @param[out] val
     * @return Status::OK found.
     * @return Status::WARN_NOT_FOUND not found.
     */
    static Status get_value(SequenceId id, value_type& val) {
        if (sm_.find(id) == sm_.end()) {
            return Status::WARN_NOT_FOUND;
        }
        val = sm_[id];
        return Status::OK;
    }

    /**
     * @pre It has acquired lock.
     */
    static value_type& get_value_ref(SequenceId id) {
        return sm_[id];
    }

    /**
     * @pre It has acquired lock.
     */
    static void put_value(SequenceId id, value_type new_v) {
        sm_[id] = new_v;
    }

    static SequenceId fetch_add_created_num() {
        SequenceId ret{created_num_.fetch_add(1)};
        if (ret == SIZE_MAX) {
            LOG(FATAL) << "fatal error"; // todo round-trip
        }
        return ret;
    }

private:
    static inline std::map<SequenceId, value_type> sm_; // NOLINT
    static inline std::atomic<SequenceId> created_num_{0};
    static inline std::mutex sm_mtx_;
};

} // namespace shirakami
