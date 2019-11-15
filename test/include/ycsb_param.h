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
const std::size_t kKeyLength = 16;

/**
 * @brief size of payload.
 */
const std::size_t kValLength = 8;

/**
 * @brief number of records.
 */
const std::size_t kCardinality = 40;

/**
 * @brief number of operations in one transaction.
 */
const std::size_t kNops = 10;

/**
 * @biref number of worker threads.
 */
const std::size_t kNthread = 2;

/**
 * @brief ratio of read. 0 - 100[%]
 */
const std::size_t kRRatio = 100;

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
const std::size_t kExecTime = 3;

} // end of namespace ycsb_param
