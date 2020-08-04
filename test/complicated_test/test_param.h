/**
 * @file test_param.h
 */

#pragma once

#include <cstddef>

#define DECLARE_ENTITY_TOKEN_FOR_EXP

namespace single_thread_test {

// How many threads would you like to spawn?
const std::size_t Nthread = 1;

// How many records would you like to insert for a thread?
const std::size_t Max_insert = (3);

// The length of key, which should be longer since this KVS does not allow
// duplicate keys.
const std::size_t Len_key = 20;

// The length of value
const std::size_t Len_val = 2;

}  // namespace single_thread_test
