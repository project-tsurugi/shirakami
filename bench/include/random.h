/**
 * @file src/include/random.h
 */

/**
 * This algorithm is originally developed
 * by David Blackman and Sebastiano Vigna (vigna@acm.org)
 * http://xoroshiro.di.unimi.it/xoroshiro128plus.c
 *
 * And Tanabe Takayuki custmized.
 */

#pragma once

#include <array>
#include <cstdint>
#include <random>

namespace shirakami {

class Xoroshiro128Plus {
public:
    Xoroshiro128Plus() { // NOLINT
        std::random_device rnd;
        s.at(0) = rnd();
        s.at(1) = splitMix64(s.at(0));
    }

    void seed(std::uint64_t seed) {
        s.at(0) = seed;
        s.at(1) = splitMix64(seed);
    }

    static uint64_t splitMix64(std::uint64_t seed) {    // NOLINT
        std::uint64_t z = (seed += 0x9e3779b97f4a7c15); // NOLINT
        z = (z ^ (z >> 30U)) * 0xbf58476d1ce4e5b9;      // NOLINT
        z = (z ^ (z >> 27U)) * 0x94d049bb133111eb;      // NOLINT
        return z ^ (z >> 31U);                          // NOLINT
    }

    static inline uint64_t rotl(const std::uint64_t x, const int k) { // NOLINT
        return (x << k) | (x >> (64 - k));                            // NOLINT
    }

    std::uint64_t next() { // NOLINT
        const uint64_t s0 = s.at(0);
        uint64_t s1 = s.at(1);
        const uint64_t result = s0 + s1;

        s1 ^= s0;
        s.at(0) = rotl(s0, 24) ^ s1 ^ (s1 << 16); // NOLINT
        // a, b
        s.at(1) = rotl(s1, 37); // NOLINT
        // c

        return result;
    }

    uint64_t operator()() { return next(); } // NOLINT

    /* This is the jump function for the generator. It is equivalent
       to 2^64 calls to next(); it can be used to generate 2^64
       non-overlapping subsequences for parallel computations. */

    [[maybe_unused]] void jump() {
        static const std::array<uint64_t, 2> JUMP{0xdf900294d8f554a5,
                                                  0x170865df4b3201fc};

        uint64_t s0 = 0;
        uint64_t s1 = 0;
        for (uint64_t i = 0; i < sizeof JUMP / sizeof JUMP.at(0);
             i++) {                                  // NOLINT
            for (std::uint32_t b = 0; b < 64; b++) { // NOLINT
                if ((JUMP.at(i) & UINT64_C(1) << b) != 0U) {
                    s0 ^= s.at(0);
                    s1 ^= s.at(1);
                }
                next();
            }
        }
        s.at(0) = s0;
        s.at(1) = s1;
    }

    /* This is the long-jump function for the generator. It is equivalent to
       2^96 calls to next(); it can be used to generate 2^32 starting points,
       from each of which jump() will generate 2^32 non-overlapping
       subsequences for parallel distributed computations. */

    [[maybe_unused]] void long_jump() {
        static const std::array<uint64_t, 2> LONG_JUMP{0xd2a98b26625eee7b,
                                                       0xdddf9b1090aa7ac1};

        uint64_t s0 = 0;
        uint64_t s1 = 0;
        for (uint64_t i = 0; i < sizeof LONG_JUMP / sizeof LONG_JUMP.at(0);
             i++) {                                  // NOLINT
            for (std::uint32_t b = 0; b < 64; b++) { // NOLINT
                if ((LONG_JUMP.at(i) & UINT64_C(1) << b) != 0U) {
                    s0 ^= s.at(0);
                    s1 ^= s.at(1);
                }
                next();
            }
        }
        s.at(0) = s0;
        s.at(1) = s1;
    }

private:
    std::array<uint64_t, 2> s; // NOLINT
};

} // namespace shirakami
