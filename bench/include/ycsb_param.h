/**
 * @file ycsb_param.h
 * @brief ycsb test parameters
 */

#pragma once

#include "cache_line_size.hh"

namespace ycsb_param {

[[maybe_unused]]
/**
 * @brief size of key.
 */
alignas(CACHE_LINE_SIZE) std::size_t kKeyLength = 8; // NOLINT

/**
 * @brief size of payload.
 */
alignas(CACHE_LINE_SIZE) std::size_t kValLength = 8; // NOLINT

/**
 * @brief number of records.
 */
alignas(CACHE_LINE_SIZE) std::size_t kCardinality = 50; // NOLINT

/**
 * @brief number of operations in one transaction.
 */
alignas(CACHE_LINE_SIZE) std::size_t kNops = 1; // NOLINT

/**
 * @biref number of worker threads.
 */
alignas(CACHE_LINE_SIZE) std::size_t kNthread = 1; // NOLINT

/**
 * @brief ratio of read. 0 - 100[%]
 */
alignas(CACHE_LINE_SIZE) std::size_t kRRatio = 100; // NOLINT

/**
 * @brief zipf skew.
 */
alignas(CACHE_LINE_SIZE) double kZipfSkew = 0; // NOLINT

/**
 * @brief cpuMHz. use for measure time.
 */
alignas(CACHE_LINE_SIZE) std::size_t kCPUMHz = 2496; // NOLINT

/**
 * @brief experimental time.
 */
alignas(CACHE_LINE_SIZE) std::size_t kExecTime = 1; // NOLINT

} // end of namespace ycsb_param
