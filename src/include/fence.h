/**
 * @file fence.h
 */

#pragma once

#include "inline.h"

INLINE void compilerFence() { asm volatile("" ::: "memory"); }
