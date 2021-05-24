/**
 * @file concurrent_queue.h
 */

#pragma once

#include <tbb/concurrent_queue.h>

namespace shirakami {

template<class T>
class concurrent_queue {
    // Begin : intel tbb
public:
    void clear() { return queue.clear(); }

    bool empty() { return queue.empty(); }

    void push(const T& elem) { queue.push(elem); }

    std::size_t size() { return queue.unsafe_size(); }

    bool try_pop(T& result) { return queue.try_pop(result); }

private:
    tbb::concurrent_queue<T> queue;
    // End : intel tbb
};

} // namespace shirakami