/**
 * @file masstree_wrapper.cc
 * @brief implement about masstree_wrapper
 */

#include "include/masstree_wrapper.hh"

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
volatile bool recovering = false;
