/**
 * @file clock.hh
 * @brief functions about clock.
 */

#pragma once

#include <cstdint>
#include <thread>

[[maybe_unused]] static bool check_clock_span(uint64_t &start, uint64_t &stop,
                                              uint64_t threshold) {
  uint64_t diff = 0;
  diff = stop - start;
  if (diff > threshold)
    return true;
  else
    return false;
}

[[maybe_unused]] static void sleepMs(size_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}
