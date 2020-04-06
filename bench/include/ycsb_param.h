/**
 * @file
 * @brief ycsb test parameters
 */

#pragma once

#include "cache_line_size.hh"
#include "header.hh"

namespace ycsb_param {

/**
 * @brief size of key.
 */
alignas(CACHE_LINE_SIZE) std::size_t kKeyLength = 8;

/**
 * @brief size of payload.
 */
alignas(CACHE_LINE_SIZE) std::size_t kValLength = 8;

/**
 * @brief number of records.
 */
alignas(CACHE_LINE_SIZE) std::size_t kCardinality = 50;

/**
 * @brief number of operations in one transaction.
 */
alignas(CACHE_LINE_SIZE) std::size_t kNops = 1;

/**
 * @biref number of worker threads.
 */
alignas(CACHE_LINE_SIZE) std::size_t kNthread = 1;

/**
 * @brief ratio of read. 0 - 100[%]
 */
alignas(CACHE_LINE_SIZE) std::size_t kRRatio = 100;

/**
 * @brief zipf skew.
 */
alignas(CACHE_LINE_SIZE) double kZipfSkew = 0;

/**
 * @brief cpuMHz. use for measure time.
 */
alignas(CACHE_LINE_SIZE) std::size_t kCPUMHz = 2496;

/**
 * @brief experimental time.
 */
alignas(CACHE_LINE_SIZE) std::size_t kExecTime = 1;

} // end of namespace ycsb_param
