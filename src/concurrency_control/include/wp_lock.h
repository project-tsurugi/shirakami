#pragma once

#include <atomic>
#include <cstdint>

namespace shirakami::wp {

class wp_lock {
public:
    static bool is_locked(std::uint64_t obj) { return obj & 1; } // NOLINT

    std::uint64_t load_obj() { return obj.load(std::memory_order_acquire); }

    void lock();

    void unlock();

private:
    /**
     * @brief lock object
     * @details One bit indicates the presence or absence of a lock. 
     * The rest represents the number of past locks.
     * The target object loaded while loading the unlocked object twice can be 
     * regarded as atomically loaded.
     */
    std::atomic<std::uint64_t> obj{0};
};

} // namespace shirakami::wp