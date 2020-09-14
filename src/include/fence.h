/**
 * @file fence.h
 */

#pragma once

#include "inline.h"

namespace shirakami {

INLINE void compilerFence() { asm volatile("":: : "memory"); }

} // namespace shirakami

