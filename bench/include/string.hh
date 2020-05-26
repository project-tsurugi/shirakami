#pragma once

#include <string>

// shirakami/src/
#include "random.hh"

static void
make_string(std::string& string)
{
    for (uint i = 0; i < string.size()-1; i++) {
        string[i] = rand() % 24 + 'a';
    }
    // if you use printf function with %s format later,
    // the end of aray must be null chara.
    string.back() = '\0';
}

static void
make_string(std::string& string, Xoroshiro128Plus& rnd)
{
    for (uint i = 0; i < string.size()-1; i++) {
      string[i] = rnd.next() % 24 + 'a';
    }
    // if you use printf function with %s format later,
    // the end of aray must be null chara.
    string.back() = '\0';
}
