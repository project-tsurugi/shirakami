#pragma once

/**
 * @file
 * @brief atomic wrapper of builtin function.
 */

/**
 * @brief atomic relaxed load.
 */
template <typename T>
[[maybe_unused]] T loadRelaxed(T& ptr) {           // NOLINT
  return __atomic_load_n(&ptr, __ATOMIC_RELAXED);  // NOLINT
}

template <typename T>
[[maybe_unused]] T loadRelaxed(T* ptr) {          // NOLINT
  return __atomic_load_n(ptr, __ATOMIC_RELAXED);  // NOLINT
}

/**
 * @brief atomic acquire load.
 */
template <typename T>
T loadAcquire(T& ptr) {                            // NOLINT
  return __atomic_load_n(&ptr, __ATOMIC_ACQUIRE);  // NOLINT
}

template <typename T>
[[maybe_unused]] T loadAcquire(T* ptr) {          // NOLINT
  return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);  // NOLINT
}

/**
 * @brief atomic relaxed store.
 */
template <typename T, typename T2>
[[maybe_unused]] void storeRelaxed(T& ptr, T2 val) {
  __atomic_store_n(&ptr, (T)val, __ATOMIC_RELAXED);  // NOLINT
}

template <typename T, typename T2>
[[maybe_unused]] void storeRelaxed(T* ptr, T2 val) {
  __atomic_store_n(ptr, (T)val, __ATOMIC_RELAXED);  // NOLINT
}

/**
 * @brief atomic release store.
 */
template <typename T, typename T2>
void storeRelease(T& ptr, T2 val) {
  __atomic_store_n(&ptr, (T)val, __ATOMIC_RELEASE);  // NOLINT
}

template <typename T, typename T2>
[[maybe_unused]] void storeRelease(T* ptr, T2 val) {
  __atomic_store_n(ptr, (T)val, __ATOMIC_RELEASE);  // NOLINT
}

/**
 * @brief atomic acq-rel cas.
 */
template <typename T, typename T2>
bool compareExchange(T& m, T& before, T2 after) {                   // NOLINT
  return __atomic_compare_exchange_n(&m, &before, (T)after, false,  // NOLINT
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}
