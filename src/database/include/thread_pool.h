#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "thread_task.h"

// shirakami/src/include/
#include "concurrent_queue.h"
#include "cpu.h"

// third_party/
#include "glog/logging.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) thread_pool {
public:
    /**
     * @brief init thread pool
    */
    static void init(std::size_t thread_pool_size = 10) { // NOLINT
        // set thread pool size
        set_thread_pool_size(thread_pool_size);

        // gen thread array
        get_threads().reset(new std::thread[get_thread_pool_size()]); // NOLINT

        // set flag
        set_running(true);

        // start worker thread
        for (std::size_t i = 0; i < get_thread_pool_size(); ++i) {
            get_threads()[i] = std::thread(worker, i);
        }
    }

    /**
     * @brief shutdown thread pool
    */
    static void fin() {
        // set flag
        set_running(false);

        // join worker thread
        for (std::size_t i = 0; i < get_thread_pool_size(); ++i) {
            get_threads()[i].join();
        }
    }

    /**
     * @brief worker thread of thread pool.
    */
    static void worker(std::size_t worker_id);

    static void push_task_queue(thread_task* tt) { task_queue_.push(tt); }

    // getter
    static bool get_running() {
        return running_.load(std::memory_order_acquire);
    }

    static std::unique_ptr<std::thread[]>& get_threads() { // NOLINT
        return threads_;
    }

    static concurrent_queue<thread_task*>& get_task_queue() {
        return task_queue_;
    }

    // setter
    static void set_running(bool tf) {
        running_.store(tf, std::memory_order_release);
    }

private:
    // getter
    static std::size_t get_thread_pool_size() { return thread_pool_size_; }

    // setter
    static void set_thread_pool_size(std::size_t sz) { thread_pool_size_ = sz; }

    /**
     * @brief # of threads of thread pool.
    */
    static inline std::size_t thread_pool_size_{}; // NOLINT

    /**
     * @brief whether thread pool runs
    */
    static inline std::atomic<bool> running_{false}; // NOLINT

    /**
     * @brief threads of thread pool
    */
    static inline std::unique_ptr<std::thread[]> threads_; // NOLINT

    /**
     * @brief task container
    */
    static inline concurrent_queue<thread_task*> task_queue_; // NOLINT
};

} // namespace shirakami