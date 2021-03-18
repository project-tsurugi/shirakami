/**
 * @file src/include/sequence.h
 */

#pragma once

#include <map>
#include <mutex>

#include "logger.h"

#include "kvs/interface.h"

using namespace shirakami::cc_silo_variant;

namespace shirakami {

class sequence_map {
public:
    /**
     * @details The first of std::pair<std::pair<SequenceVersion, SequenceValue>, std::pair<SequenceVersion, SequenceValue>> is not durable, and the second of that is durable.
     */
    using value_type = std::pair<std::pair<SequenceVersion, SequenceValue>, std::pair<SequenceVersion, SequenceValue>>;
    static constexpr std::pair<SequenceVersion, SequenceValue> initial_value{1, 0};
    static constexpr std::pair<SequenceVersion, SequenceValue> non_exist_value{0, 0};
    static constexpr std::pair<SequenceVersion, SequenceValue> deleted_value{SIZE_MAX, 0};
    static constexpr std::size_t version_pos{0};
    static constexpr std::size_t value_pos{1};

    static std::map<SequenceId, value_type>& get_sm() {
        return sm_;
    }

    static SequenceId fetch_add_created_num() {
        SequenceId ret = created_num_;
        ++created_num_;
        if (created_num_ == SIZE_MAX) {
            SPDLOG_DEBUG("fatal error");
            exit(1);
        }
        return ret;
    }

    static std::mutex& get_smmutex() {
        return smmutex_;
    }

private:
    static inline std::map<SequenceId, value_type> sm_;
    static inline SequenceId created_num_{0};
    static inline std::mutex smmutex_;
};

} // namespace shirakami
