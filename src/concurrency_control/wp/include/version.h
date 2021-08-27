/**
 * @file concurrency_control/wp/include/version.h
 */

#include "cpu.h"

#include <atomic>

namespace shirakami {

class alignas(CACHE_LINE_SIZE) version {
public:
    std::string* get_pv() {
        return pv_.load(std::memory_order_acquire);
    }

    void set_pv(std::string_view vinfo) {
        if (get_pv() == nullptr) {
            pv_.store(new std::string(vinfo), std::memory_order_release);
        } else {
            
        }
    }

private:
    /**
     * @brief pointer to value.
     */
    std::atomic<std::string*> pv_{nullptr};
    /**
     * @brief pointer to next version.
     */
    std::atomic<version*> next_{nullptr};
};
}