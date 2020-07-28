#pragma once

#include <string>

// shirakami/src/
#include "random.h"

namespace shirakami {

static void make_string(std::string& string, Xoroshiro128Plus& rnd) {
  for (std::size_t i = 0; i < string.size() - 1; i++) {
    constexpr std::size_t tf = 24;
    string[i] = rnd.next() % tf + 'a';
  }
  // if you use printf function with %s format later,
  // the end of aray must be null chara.
  string.back() = '\0';
}

}  // namespace shirakami
