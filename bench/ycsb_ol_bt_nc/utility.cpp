
#include <iostream>
#include <vector>

#include "declare_gflags.h"
#include "simple_result.h"

#include "memory.h"

#include "shirakami/logging.h"

#include "glog/logging.h"

using namespace shirakami;

void check_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_cpumhz > 1) {
        printf("FLAGS_cpumhz : %zu\n", FLAGS_cpumhz); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "CPU MHz of execution environment. It is used measuring "
                      "some time. It must be larger than 0.";
    }
    if (FLAGS_duration >= 1) {
        printf("FLAGS_duration : %zu\n", FLAGS_duration); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Duration of benchmark in seconds must be larger than 0.";
    }
    if (FLAGS_key_len > 0) {
        printf("FLAGS_key_len : %zu\n", FLAGS_key_len); // NOLINT
    }
    if (FLAGS_val_len > 1) {
        printf("FLAGS_val_len : %zu\n", FLAGS_val_len); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Length of val must be larger than 0.";
    }

    std::cout << "online options" << std::endl;
    printf("FLAGS_ol_ops : %zu\n", FLAGS_ol_ops);           // NOLINT
    if (FLAGS_ol_rratio >= 0 && FLAGS_ol_rratio <= 100) {   // NOLINT
        printf("FLAGS_ol_rratio : %zu\n", FLAGS_ol_rratio); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Rate of reads in a transaction must be in the range 0 "
                      "to 100.";
    }
    printf("FLAGS_ol_rec : %zu\n", FLAGS_ol_rec); // NOLINT
    if (FLAGS_ol_skew >= 0 && FLAGS_ol_skew < 1) {
        std::cout << "FLAGS_ol_skew:\t" << FLAGS_ol_skew << std::endl;
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Access skew of transaction must be in the range 0 to "
                      "0.999... .";
    }
    std::cout << "FLAGS_ol_thread:\t" << FLAGS_ol_thread << std::endl;

    std::cout << "batch options" << std::endl;
    printf("FLAGS_bt_ops : %zu\n", FLAGS_bt_ops);           // NOLINT
    if (FLAGS_bt_rratio >= 0 && FLAGS_bt_rratio <= 100) {   // NOLINT
        printf("FLAGS_bt_rratio : %zu\n", FLAGS_bt_rratio); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Rate of reads in a transaction must be in the range 0 "
                      "to 100.";
    }
    printf("FLAGS_bt_rec : %zu\n", FLAGS_bt_rec); // NOLINT
    if (FLAGS_bt_skew >= 0 && FLAGS_bt_skew < 1) {
        std::cout << "FLAGS_bt_skew:\t" << FLAGS_bt_skew << std::endl;
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Access skew of transaction must be in the range 0 to "
                      "0.999... .";
    }
    std::cout << "FLAGS_bt_thread:\t" << FLAGS_bt_thread << std::endl;

    printf("Fin check_flags()\n"); // NOLINT
}

void output_result(std::vector<simple_result> const& res_ol,
                   std::vector<simple_result> const& res_bt) {
    std::uint64_t ol_ct_commit{0};
    std::uint64_t ol_ct_abort{0};
    for (auto&& elem : res_ol) {
        ol_ct_abort += elem.get_ct_abort();
        ol_ct_commit += elem.get_ct_commit();
    }
    std::cout << "ol_abort_rate:\t"
              << static_cast<double>(ol_ct_abort) /
                         static_cast<double>(ol_ct_commit + ol_ct_abort)
              << std::endl;
    std::cout << "ol_abort_count:\t" << ol_ct_abort << std::endl;
    std::cout << "ol_throughput[tps]:\t" << ol_ct_commit / FLAGS_duration
              << std::endl;
    std::cout << "ol_throughput[ops/s]:\t"
              << (ol_ct_commit * FLAGS_ol_ops) / FLAGS_duration << std::endl;
    std::cout << "ol_throughput[ops/s/th]:\t"
              << (ol_ct_commit * FLAGS_ol_ops) / FLAGS_duration /
                         FLAGS_ol_thread
              << std::endl;

    std::uint64_t bt_ct_commit{0};
    std::uint64_t bt_ct_abort{0};
    for (auto&& elem : res_bt) {
        bt_ct_abort += elem.get_ct_abort();
        bt_ct_commit += elem.get_ct_commit();
    }
    std::cout << "bt_abort_rate:\t"
              << static_cast<double>(bt_ct_abort) /
                         static_cast<double>(bt_ct_commit + bt_ct_abort)
              << std::endl;
    std::cout << "bt_abort_count:\t" << bt_ct_abort << std::endl;
    std::cout << "bt_throughput[tps]:\t" << bt_ct_commit / FLAGS_duration
              << std::endl;
    std::cout << "bt_throughput[ops/s]:\t"
              << (bt_ct_commit * FLAGS_bt_ops) / FLAGS_duration << std::endl;
    std::cout << "bt_throughput[ops/s/th]:\t"
              << (bt_ct_commit * FLAGS_bt_ops) / FLAGS_duration /
                         FLAGS_bt_thread
              << std::endl;

    shirakami::displayRusageRUMaxrss();
#if defined(CPR)
    printf("cpr_global_version:\t%zu\n",
           shirakami::cpr::global_phase_version::get_gpv()
                   .get_version()); // NOLINT
#endif
}