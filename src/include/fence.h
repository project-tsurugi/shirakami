/**
 * @file fence.hh
 */

#pragma once

#include "inline.h"

INLINE void compilerFence() { asm volatile("" ::: "memory"); }
