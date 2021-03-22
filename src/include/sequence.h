/**
 * @file src/include/sequence.h
 */

#pragma once

#include <map>
#include <mutex>

#include "logger.h"

#include "kvs/interface.h"

#if defined(CPR)
#include "cpr.h"
#endif

using namespace shirakami::logger;

namespace shirakami {

class sequence_map {
public:
    /**
     * @details The first of std::pair<std::pair<SequenceVersion, SequenceValue>, std::pair<SequenceVersion, SequenceValue>> is not durable, and the second of that is durable.
     */
#if defined(CPR)
    using value_type = std::tuple<std::pair<SequenceVersion, SequenceValue>, std::pair<SequenceVersion, SequenceValue>, cpr::version_type>;
    static constexpr std::pair<SequenceVersion, SequenceValue> initial_value{1, 0};
    static constexpr std::pair<SequenceVersion, SequenceValue> non_exist_value{0, 0};
    static constexpr std::pair<SequenceVersion, SequenceValue> deleted_value{SIZE_MAX, 0};
    static constexpr std::size_t cpr_version_pos{2};
    static constexpr value_type non_exist_map_value{non_exist_value, non_exist_value, 0};
#else
    using value_type = std::pair<std::pair<SequenceVersion, SequenceValue>, std::pair<SequenceVersion, SequenceValue>>;
    static constexpr std::pair<SequenceVersion, SequenceValue> initial_value{1, 0};
    static constexpr std::pair<SequenceVersion, SequenceValue> non_exist_value{0, 0};
    static constexpr std::pair<SequenceVersion, SequenceValue> deleted_value{SIZE_MAX, 0};
    static constexpr value_type non_exist_map_value{non_exist_value, non_exist_value};
#endif
    static constexpr std::size_t volatile_pos{0};
    static constexpr std::size_t durable_pos{1};
    static constexpr std::size_t version_pos{0};
    static constexpr std::size_t value_pos{1};

    /**
     * @pre It is called by create_sequence function.
     */
    static void create_initial_value(SequenceId id) {
        std::unique_lock lock{sequence_map::get_smmutex()};
#if defined(CPR)
        sequence_map::get_sm()[id] = sequence_map::value_type{sequence_map::initial_value, sequence_map::non_exist_value, 0};
#else
        sequence_map::get_sm()[id] = sequence_map::value_type{sequence_map::initial_value, sequence_map::non_exist_value};
#endif
    }

    static std::map<SequenceId, value_type>& get_sm() {
        return sm_;
    }

    static std::mutex& get_smmutex() {
        return smmutex_;
    }

    /**
     * @pre It has acquired lock.
     */
    static value_type& get_value(SequenceId id) {
        return sm_[id];
    }

    /**
     * @pre It has acquired lock.
     */
    static void put_value(SequenceId id, value_type new_v) {
        sm_[id] = new_v;
    }

    static SequenceId fetch_add_created_num() {
        SequenceId ret = created_num_;
        ++created_num_;
        if (created_num_ == SIZE_MAX) {
            shirakami_logger->debug("fatal error"); // todo round-trip
            exit(1);
        }
        return ret;
    }

private:
    static inline std::map<SequenceId, value_type> sm_; // NOLINT
    static inline SequenceId created_num_{0};
    static inline std::mutex smmutex_;
};

} // namespace shirakami
