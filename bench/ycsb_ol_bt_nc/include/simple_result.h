/**
 * @file simple_result.h
 */

#pragma once

#include <cstdint>

namespace shirakami {

class alignas(64) simple_result { // NOLINT
public:
    // getter
    [[nodiscard]] double get_abort_rate() const { return abort_rate_; }

    [[nodiscard]] std::uint64_t get_ct_abort() const { return ct_abort_; }

    [[nodiscard]] std::uint64_t get_ct_commit() const { return ct_commit_; }

    [[nodiscard]] std::uint64_t get_ct_cpr() const { return ct_cpr_; }

    [[nodiscard]] std::uint64_t get_maxrss() const { return maxrss_; }

    [[nodiscard]] double get_throughput() const { return throughput_; }

    // setter
    void set_abort_rate(double rate) { abort_rate_ = rate; }

    void set_ct_abort(std::uint64_t ct) { ct_abort_ = ct; }

    void set_ct_commit(std::uint64_t ct) { ct_commit_ = ct; }

    void set_ct_cpr(std::uint64_t ct) { ct_cpr_ = ct; }

    void set_maxrss(std::uint64_t maxrss) { maxrss_ = maxrss; }

    void set_throughput(double throughput) { throughput_ = throughput; }

private:
    double abort_rate_;
    std::uint64_t ct_abort_;
    std::uint64_t ct_commit_;
    std::uint64_t ct_cpr_;
    std::uint64_t maxrss_;
    double throughput_;
};

} // namespace shirakami