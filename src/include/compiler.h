/**
 * @file compiler.h
 */

#pragma once

namespace shirakami {

#define likely(x) __builtin_expect(!!(x), 1)   // LINT
#define unlikely(x) __builtin_expect(!!(x), 0) // LINT

#define STRING(macro) #macro         // LINT
#define MAC2STR(macro) STRING(macro) // LINT

} // namespace shirakami
