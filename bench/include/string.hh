#pragma once

// kvs_charkey/src/
#include "include/header.hh"

static void
make_string(char* string, std::size_t len)
{
    for (uint i = 0; i < len-1; i++) {
        string[i] = rand() % 24 + 'a';
    }
    // if you use printf function with %s format later,
    // the end of aray must be null chara.
    string[len-1] = '\0';
}
