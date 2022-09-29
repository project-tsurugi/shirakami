#pragma once

#include <cstdint>
#include <shared_mutex>
#include <tuple>
#include <vector>

#include "epoch.h"

#include "shirakami/api_sequence.h"

namespace shirakami {

class sequence {
public:
    static constexpr std::size_t initial_id_counter{0};

    /**
     * @brief When recovery is not enable, this is used for scratch-startup.
     * 
     */
    static void init() {
        id_counter_.store(initial_id_counter, std::memory_order_release);
    }

    /**
     * @brief Set the id counter object
     * @details 
     * @param id 
     */
    static void set_id_counter(SequenceId id) {
        id_counter_.store(id, std::memory_order_release);
    }

private:
    /**
     * @brief This is used for deciding unique sequence id efficiently.
     * There is a approach which it tries generation sequence id from 0, but it 
     * can be many duplication. For avoiding that, sequence id generation use
     * it with fetch_add instruction.
     */
    static inline std::atomic<SequenceId> id_counter_; // NOLINT
};

class sequence_object {
public:
private:
    std::shared_mutex smtx_;
    bool is_hooked_{false}; // NOLINT
    std::vector<std::tuple<SequenceVersion, SequenceValue, epoch::epoch_t>>
            body_;
};

} // namespace shirakami