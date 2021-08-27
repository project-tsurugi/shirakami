/**
 * @file lock.h
 * @brief locks for wp protocols
 */

// c library
#include <xmmintrin.h>

// cxx library
#include <atomic>

namespace shirakami {

/**
 * @brief mutex for wp
 * @details Current std (mutex) library of cxx does not serve checking whether the lock is locked without locking the lock.
 * If the property exists, we reduce cache-line conflicts about operating the lock.
 * So i design a lock with the property.
 */
class s_mutex {
public:

    /**
     * @brief check whether the lock is locked.
     * @return true This is locked.
     * @return false This is not locked.
     */
    bool get_locked() {
        return locked_.load(std::memory_order_acquire);
    }

    void lock(){
        for (;;) {
            bool expected{false};
            bool desired{true};
            if (locked_.compare_exchange_weak(expected, desired, std::memory_order_release, std::memory_order_acquire)) break;
            _mm_pause();
        }
    }

    /**
     * @brief unlock the lock.
     * @pre caller locked this lock.
     */
    void unlock(){
        locked_.store(false, std::memory_order_release);
    }

private:
    std::atomic<bool> locked_{false};
};

} // namespace shirakami