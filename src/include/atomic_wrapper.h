#pragma once

/**
 * @file
 * @brief atomic wrapper of builtin function.
 */

namespace shirakami {

/**
 * @brief atomic relaxed load.
 */
template<typename T>
[[maybe_unused]] static T loadRelaxed(T& ptr) {     // LINT
    return __atomic_load_n(&ptr, __ATOMIC_RELAXED); // LINT
}

template<typename T>
[[maybe_unused]] static T loadRelaxed(T* ptr) {    // LINT
    return __atomic_load_n(ptr, __ATOMIC_RELAXED); // LINT
}

/**
 * @brief atomic acquire load.
 */
template<typename T>
static T loadAcquire(T& ptr) {                      // LINT
    return __atomic_load_n(&ptr, __ATOMIC_ACQUIRE); // LINT
}

template<typename T>
[[maybe_unused]] static T loadAcquire(T* ptr) {    // LINT
    return __atomic_load_n(ptr, __ATOMIC_ACQUIRE); // LINT
}

/**
 * @brief atomic relaxed store.
 */
template<typename T, typename T2>
[[maybe_unused]] static void storeRelaxed(T& ptr, T2 val) {
    __atomic_store_n(&ptr, (T) val, __ATOMIC_RELAXED); // LINT
}

template<typename T, typename T2>
[[maybe_unused]] static void storeRelaxed(T* ptr, T2 val) {
    __atomic_store_n(ptr, (T) val, __ATOMIC_RELAXED); // LINT
}

/**
 * @brief atomic release store.
 */
template<typename T, typename T2>
static void storeRelease(T& ptr, T2 val) {
    __atomic_store_n(&ptr, (T) val, __ATOMIC_RELEASE); // LINT
}

template<typename T, typename T2>
[[maybe_unused]] static void storeRelease(T* ptr, T2 val) {
    __atomic_store_n(ptr, (T) val, __ATOMIC_RELEASE); // LINT
}

/**
 * @brief atomic acq-rel cas.
 */
template<typename T, typename T2>
static bool compareExchange(T& m, T& before, T2 after) {              // LINT
    return __atomic_compare_exchange_n(&m, &before, (T) after, false, // LINT
                                       __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

} // namespace shirakami
