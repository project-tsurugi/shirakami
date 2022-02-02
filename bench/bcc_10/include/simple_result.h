/**
 * @file simple_result.h
 */

#pragma once

#include <cstdint>

class alignas(64) simple_result { // NOLINT
public:
    // getter
    [[nodiscard]] std::uint64_t get_ct_commit() const { return ct_commit_; }

    // setter
    void set_ct_commit(std::uint64_t ct) { ct_commit_ = ct; }

private:
    std::uint64_t ct_commit_{0};
};
