/**
 * @file src/include/sequence.h
 */

#pragma once

#include <array>
#include <atomic>
#include <map>
#include <shared_mutex>

#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/interface.h"
#include "shirakami/scheme.h"

#include <glog/logging.h>

namespace shirakami {

class sequence {
public:
    /**
     * @details epoch::epoch_t is durable epoch, std::tuple<SequenceVersion, 
     * SequenceValue> is the pair information of that.
     */
    using value_type = std::map<epoch::epoch_t,
                                std::tuple<SequenceVersion, SequenceValue>>;
    static constexpr std::tuple<SequenceVersion, SequenceValue> initial_value{
            0, 0};
    static constexpr std::tuple<SequenceVersion, SequenceValue> non_exist_value{
            SIZE_MAX, 0};
    static constexpr std::size_t version_pos{0};
    static constexpr std::size_t value_pos{1};

    static void init() {
        sequence_map().clear();
        id_generator_ctr().store(0, std::memory_order_release);
    }

    /**
     * @brief Create a sequence object
     * @param[out] id 
     * @return Status 
     */
    static Status create_sequence(SequenceId* id);

    /**
     * @brief It updates sequence object.
     * @param[in] token 
     * @param[in] id 
     * @param[in] version 
     * @param[in] value 
     * @return Status 
     */
    static Status update_sequence(Token token, SequenceId id,
                                  SequenceVersion version, SequenceValue value);

    /**
     * @brief It reads sequence object.
     * @param[in] id 
     * @param[out] version 
     * @param[out] value 
     * @return Status 
     */
    static Status read_sequence(SequenceId id, SequenceVersion* version,
                                SequenceValue* value);

    /**
     * @brief It deletes sequence object.
     * @param[in] id 
     * @return Status 
     */
    static Status delete_sequence(SequenceId id);

    // getter / setter
    static std::shared_mutex& sequence_map_smtx() { return sequence_map_smtx_; }

    static std::atomic<SequenceId>& id_generator_ctr() {
        return id_generator_ctr_;
    }

    static std::map<SequenceId, value_type>& sequence_map() {
        return sequence_map_;
    }

private:
    static inline std::map<SequenceId, value_type> sequence_map_; // NOLINT
    static inline std::atomic<SequenceId> id_generator_ctr_{0};
    static inline std::shared_mutex sequence_map_smtx_;
};

} // namespace shirakami
