/**
 * @file wp/include/epoch.h
 */

#pragma once

#include <atomic>
#include <mutex>
#include <thread>

#include "database/include/tx_state_notification.h"

#include "glog/logging.h"

namespace shirakami::epoch {

using epoch_t = std::uint64_t;

static constexpr epoch_t initial_epoch{1};

static constexpr epoch_t initial_cc_safe_ss_epoch{initial_epoch + 1};

static constexpr epoch_t max_epoch{INT64_MAX};

/**
 * @brief global epoch
 * @pre We start with 1 because we give 0 the meaning of uninitialized.
 */
inline std::atomic<epoch_t> global_epoch{initial_epoch}; // LINT

/**
 * @brief Global epoch time. Default is 40 ms == 40,000 us.
 */
inline std::atomic<std::size_t> global_epoch_time_us{40 * 1000}; // LINT

/**
 * @brief safe snapshot epoch in the viewpoint of concurrency control.
 */
inline std::atomic<epoch_t> cc_safe_ss_epoch{// LINT
                                             initial_cc_safe_ss_epoch};

/**
 * @brief minimum epoch that OCC might potentially write to.
 * @details this is the cache value of min(session::short_expose_ongoing_status.target_epoch).
 */
inline std::atomic<epoch_t> min_epoch_occ_potentially_write{0};

inline std::atomic<epoch_t> datastore_durable_epoch{0}; // LINT

[[maybe_unused]] inline std::thread epoch_thread; // LINT

[[maybe_unused]] inline std::atomic<bool> epoch_thread_end; // LINT

[[maybe_unused]] inline std::mutex ep_mtx_; // LINT

[[maybe_unused]] static epoch_t get_datastore_durable_epoch() { // LINT
    return datastore_durable_epoch.load(std::memory_order_acquire);
}

[[maybe_unused]] static bool get_epoch_thread_end() { // LINT
    return epoch_thread_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static std::mutex& get_ep_mtx() { return ep_mtx_; } // LINT

[[maybe_unused]] static epoch_t get_global_epoch() { // LINT
    return global_epoch.load(std::memory_order_acquire);
}

[[maybe_unused]] static std::size_t get_global_epoch_time_us() { // LINT
    return global_epoch_time_us.load(std::memory_order_acquire);
}

[[maybe_unused]] static epoch_t get_cc_safe_ss_epoch() { // LINT
    return cc_safe_ss_epoch.load(std::memory_order_acquire);
}

[[maybe_unused]] static epoch_t get_min_epoch_occ_potentially_write() { // LINT
    return min_epoch_occ_potentially_write.load(std::memory_order_acquire);
}

[[maybe_unused]] static void join_epoch_thread() { epoch_thread.join(); }

[[maybe_unused]] static void set_epoch_thread_end(const bool tf) {
    epoch_thread_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_global_epoch(const epoch_t epo) { // LINT
    global_epoch.store(epo, std::memory_order_release);
}

[[maybe_unused]] static void set_global_epoch_time_us(const std::size_t num) {
    global_epoch_time_us.store(num, std::memory_order_release);
}

[[maybe_unused]] static void set_datastore_durable_epoch(epoch_t ep) {
    datastore_durable_epoch.store(ep, std::memory_order_release);
    call_durability_callbacks(ep);
}

[[maybe_unused]] static void set_cc_safe_ss_epoch(const epoch_t ep) {
    cc_safe_ss_epoch.store(ep, std::memory_order_release);
}

[[maybe_unused]] static void set_min_epoch_occ_potentially_write(const epoch_t ep) {
    min_epoch_occ_potentially_write.store(ep, std::memory_order_release);
}

// update if min_epoch_occ_potentially_write is less than ep
[[maybe_unused]] static void advance_min_epoch_occ_potentially_write(const epoch_t ep) {
    auto expected = min_epoch_occ_potentially_write.load(std::memory_order_acquire);
    while (true) {
        if (expected >= ep) { break; } // someone updated to greater value
        if (min_epoch_occ_potentially_write.compare_exchange_weak(
                    expected, ep, std::memory_order_release, std::memory_order_acquire)) {
            return; // success
        }
    }
}

// For DEBUG and TEST
//==========
using ptp_body_type = int;
static constexpr ptp_body_type ptp_init_val{-1};

/**
 * @brief For debug and test: permission to proceed epoch
 * @details If this is -1, this variable is invalid. If not, epoch manager can
 * proceed epoch for the value of this.
 */
inline std::atomic<ptp_body_type> perm_to_proc_{ptp_init_val}; // LINT

[[maybe_unused]] static void set_perm_to_proc(ptp_body_type num) {
    perm_to_proc_.store(num, std::memory_order_release);
}

[[maybe_unused]] static ptp_body_type get_perm_to_proc() {
    return perm_to_proc_.load(std::memory_order_acquire);
}

//==========

} // namespace shirakami::epoch
