/**
 * @file masstree_wrapper.cc
 * @brief implement about masstree_wrapper
 */

#include "masstree_beta_wrapper.h"

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
[[maybe_unused]] volatile bool recovering = false;
