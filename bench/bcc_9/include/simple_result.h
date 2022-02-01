/**
 * @file simple_result.h
 */

#pragma once

#include <cstdint>

class alignas(64) simple_result { // NOLINT
public:
    // getter
    [[nodiscard]] std::uint64_t get_ct_abort() const { return ct_abort_; }

    [[nodiscard]] std::uint64_t get_ct_commit() const { return ct_commit_; }

    // setter
    void set_ct_abort(std::uint64_t ct) { ct_abort_ = ct; }

    void set_ct_commit(std::uint64_t ct) { ct_commit_ = ct; }

private:
    std::uint64_t ct_abort_{0};

    std::uint64_t ct_commit_{0};
};
