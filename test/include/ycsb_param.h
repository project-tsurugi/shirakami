/**
 * @file
 * @brief ycsb test parameters
 */

#pragma once

#include "include/header.hh"

namespace ycsb_param {

/**
 * @brief size of key.
 */
const std::size_t kKeyLength = 8;

/**
 * @brief size of payload.
 */
const std::size_t kValLength = 8;

/**
 * @brief number of records.
 */
const std::size_t kCardinality = 50;

/**
 * @brief number of operations in one transaction.
 */
const std::size_t kNops = 10;

/**
 * @biref number of worker threads.
 */
const std::size_t kNthread = 1;

/**
 * @brief ratio of read. 0 - 100[%]
 */
const std::size_t kRRatio = 50;

/**
 * @brief zipf skew.
 */
const double kZipfSkew = 0;

/**
 * @brief cpuMHz. use for measure time.
 */
const std::size_t kCPUMHz = 2496;

/**
 * @brief experimental time.
 */
const std::size_t kExecTime = 1;

} // end of namespace ycsb_param
