/**
 * @file src/include/zipf.h
 */

#pragma once

#include <cassert>
#include <cfloat>
#include <cmath>
#include <iostream>
#include <random>
#include <vector>

#include "random.h"

#include "glog/logging.h"

namespace shirakami {

// Fast zipf distribution by Jim Gray et al.
class FastZipf {
    Xoroshiro128Plus* rnd_;
    const size_t nr_;
    const double alpha_, zetan_, eta_;
    const double threshold_;

public:
    FastZipf(Xoroshiro128Plus* const rnd, const double theta,
             const std::size_t nr)
        : rnd_(rnd), nr_(nr), alpha_(1.0 / (1.0 - theta)),
          zetan_(zeta(nr, theta)),
          eta_((1.0 - std::pow(2.0 / static_cast<double>(nr), // NOLINT
                               1.0 - theta)) /                // NOLINT
               (1.0 - zeta(2, theta) / zetan_)),
          threshold_(1.0 + std::pow(0.5, theta)) { // NOLINT
        assert(0.0 <= theta); // NOLINT
        assert(theta < 1.0); // NOLINT
        // 1.0 can not be specified.
    }

    // Use this constructor if zeta is pre-calculated.
    FastZipf(Xoroshiro128Plus* const rnd, const double theta,
             const std::size_t nr, const double zetan)
        : rnd_(rnd), nr_(nr), alpha_(1.0 / (1.0 - theta)), zetan_(zetan),
          eta_((1.0 - std::pow(2.0 / static_cast<double>(nr), // NOLINT
                               1.0 - theta)) /                // NOLINT
               (1.0 - zeta(2, theta) / zetan_)),
          threshold_(1.0 + std::pow(0.5, theta)) { // NOLINT
        assert(0.0 <= theta);                      // NOLINT
        assert(theta < 1.0);                       // NOLINT
        // 1.0 can not be specified.
    }

    size_t operator()() { // NOLINT
        double u = rnd_->next() / static_cast<double> UINT64_MAX;
        double uz = u * zetan_;
        if (uz < 1.0) return 0;
        if (uz < threshold_) return 1;
        return static_cast<size_t>(static_cast<double>(nr_) *
                                   std::pow(eta_ * u - eta_ + 1.0, alpha_));
    }

    [[maybe_unused]] std::uint64_t rand() { return rnd_->next(); } // NOLINT

    static double zeta(const std::size_t nr, const double theta) { // NOLINT
        double ans = 0.0;
        for (size_t i = 0; i < nr; ++i) {
            ans += std::pow(1.0 / static_cast<double>(i + 1), theta);
        }
        return ans;
    }
};

} // namespace shirakami
